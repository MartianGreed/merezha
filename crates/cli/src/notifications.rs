//! `merezha notifications check --config merezha.toml`: the notifications
//! block's startup validation (NT §4, §7.2) without starting anything. It
//! loads the project like `merezha run`, kernel sections and every block of
//! this binary included (so the other blocks' sections validate too), and
//! prints one JSON report on stdout: `{"status":"ok",...}` and exit 0, or
//! `{"status":"invalid","issues":[{"key","code","origin"}]}` and exit 2.
//! An ok report lists `warnings` (`[{key, code}]`), such as
//! `security_mail_disabled` with `email.driver = "disabled"`. Issues and
//! warnings name settings and template files, never values.
//!
//! `merezha notifications brevo register-webhook --config merezha.toml
//! [--url URL [--allow-host]]` (NT §5.2, NT-N3) validates the project the
//! same way, then
//! creates Brevo's account-level transactional webhook with `POST
//! /v3/webhooks`: the email events of the callback, `auth: { type: bearer,
//! token: brevo.webhook_token }`, to `URL` or, by default,
//! `public_base_url` + `/v1/notifications/providers/brevo/email`. A given
//! `URL` must have that path and `public_base_url`'s host and port;
//! `--allow-host` lets another host through with the `host_override`
//! warning (in `warnings` and on stderr). It prints
//! `{"status":"registered","id":N,"url":"…","warnings":[…]}` and exits 0, or
//! `{"status":"failed","code":"…"}` and exits 1 (`brevo_not_configured`
//! without a Brevo email driver, `missing_public_base_url`, `invalid_url`,
//! `callback_path_mismatch`, `callback_host_mismatch`, `provider_account`,
//! `provider_rejected[_<code>]`, or a transport code).
//! The API key and the token are never printed.
//!
//! `merezha notifications vapid generate` (NT §5.3, NT-N6) prints a new
//! VAPID key pair as one JSON object and exits 0: `private_key` (base64url
//! of the 32-byte P-256 scalar, the value of `push.vapid_private_key`),
//! `private_key_pem` (the same key as PKCS#8), `public_key` (base64url of
//! the uncompressed point, the browser's `applicationServerKey`) and
//! `key_id`. It reads no configuration and writes nothing else; store the
//! private key as a secret (`{ env = "…" }` or `{ file = "…" }`).
//!
//! `merezha notifications purge-organization ORGANIZATION --config PATH
//! [--force]` (issue #63) marks the organization's tenant deleted and runs
//! the block's purge to the end, for organizations identity deleted before
//! the block consumed `identity.organization_deleted`, or a missed event,
//! as `merezha content purge-organization` does for content. It refuses
//! (exit 1) an organization identity still has, or one identity cannot
//! confirm deleted, unless `--force`, which prints a `warning: --force:`
//! line first. It logs `notification.organization_deleted` (when it
//! marked the tenant) and `notification.organization_purged`, source
//! `operator`; exit 2 means bad arguments, configuration or database
//! (`notification.purge_failed`).
use merezha_notifications::{HostFacts, Notifications};
use merezha_platform::{ConfigInput, host};
use serde_json::{Value, json};
use std::process::ExitCode;

pub const USAGE: &str = "merezha notifications check --config PATH [--listen ADDRESS] \
     [--set SECTION.KEY=VALUE]... | merezha notifications brevo register-webhook --config PATH \
     [--url URL [--allow-host]] [--set SECTION.KEY=VALUE]... | merezha notifications vapid generate \
     | merezha notifications purge-organization ORGANIZATION --config PATH [--force] \
     [--set SECTION.KEY=VALUE]...";
/// The usage of `merezha notifications purge-organization`.
pub const PURGE_USAGE: &str = "merezha notifications purge-organization ORGANIZATION --config PATH [--force] [--set SECTION.KEY=VALUE]...";
/// Bound on each purge transaction of the command.
const PURGE_STEP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// How long `brevo register-webhook` waits for Brevo.
const REGISTER_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// The report and exit code of a check, for embedding and tests.
pub fn check(input: &ConfigInput) -> (Value, ExitCode) {
    let module = Notifications::new(HostFacts::load(input));
    let handle = module.handle();
    match host::configure(input, crate::blocks_with_notifications(input, module)) {
        Ok(_) => match handle.checked() {
            Some(checked) => {
                let settings = &checked.settings;
                let warnings: Vec<Value> = checked
                    .warnings()
                    .into_iter()
                    .map(|(key, code)| json!({"key": key, "code": code}))
                    .collect();
                let report = json!({
                    "status": "ok",
                    "topics": settings.topics.len(),
                    "templates": checked.registry.templates().count(),
                    "locales": settings.templates.locales,
                    "email": settings.email.driver.as_str(),
                    "sms": settings.sms.driver.as_str(),
                    "push": settings.push.is_some(),
                    "webhooks": settings.webhooks.is_some(),
                    "warnings": warnings,
                });
                (report, ExitCode::SUCCESS)
            }
            None => (
                json!({"status": "invalid", "issues": [
                    {"key": "modules.notifications", "code": "missing_required", "origin": "default"}
                ]}),
                ExitCode::from(2),
            ),
        },
        Err(errors) => {
            let issues: Vec<Value> = errors
                .0
                .iter()
                .map(|i| json!({"key": i.key, "code": i.code, "origin": i.origin.to_string()}))
                .collect();
            (
                json!({"status": "invalid", "issues": issues}),
                ExitCode::from(2),
            )
        }
    }
}

/// Entry point for `merezha notifications ...`.
pub fn main(mut args: impl Iterator<Item = String>) -> ExitCode {
    let invalid = || {
        println!("{}", json!({"status": "invalid_arguments", "usage": USAGE}));
        ExitCode::from(2)
    };
    let register = match args.next().as_deref() {
        Some("check") => false,
        Some("brevo") if args.next().as_deref() == Some("register-webhook") => true,
        Some("purge-organization") => return purge_main(args),
        Some("vapid") => {
            if args.next().as_deref() != Some("generate") || args.next().is_some() {
                return invalid();
            }
            let (report, code) = vapid_generate();
            println!("{report}");
            return code;
        }
        _ => return invalid(),
    };
    let mut url = None;
    let mut allow_host = false;
    let mut rest = Vec::new();
    while let Some(argument) = args.next() {
        if register && argument == "--url" && url.is_none() {
            match args.next() {
                Some(value) => url = Some(value),
                None => return invalid(),
            }
        } else if register && argument == "--allow-host" {
            allow_host = true;
        } else {
            rest.push(argument);
        }
    }
    let Ok(arguments) = crate::run::arguments(rest) else {
        return invalid();
    };
    let input = ConfigInput {
        path: arguments.config,
        env: std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect(),
        overrides: arguments.overrides,
    };
    let (report, code) = if register {
        register_webhook(&input, url.as_deref(), allow_host)
    } else {
        check(&input)
    };
    println!("{report}");
    code
}

/// The report and exit code of `brevo register-webhook`, for embedding
/// and tests.
pub fn register_webhook(
    input: &ConfigInput,
    url: Option<&str>,
    allow_host: bool,
) -> (Value, ExitCode) {
    let failed = |code: &str| (json!({"status": "failed", "code": code}), ExitCode::from(1));
    let module = Notifications::new(HostFacts::load(input));
    let handle = module.handle();
    if let Err(errors) = host::configure(input, crate::blocks_with_notifications(input, module)) {
        let issues: Vec<Value> = errors
            .0
            .iter()
            .map(|i| json!({"key": i.key, "code": i.code, "origin": i.origin.to_string()}))
            .collect();
        return (
            json!({"status": "invalid", "issues": issues}),
            ExitCode::from(2),
        );
    }
    let Some(checked) = handle.checked() else {
        return failed(merezha_notifications::brevo::NOT_CONFIGURED);
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => return failed("internal"),
    };
    let result = runtime.block_on(async {
        let http = merezha_platform::OutboundHttp::new()?;
        merezha_notifications::brevo::register_webhook(
            &checked.settings,
            url,
            allow_host,
            &http,
            std::time::Instant::now() + REGISTER_DEADLINE,
        )
        .await
    });
    match result {
        Ok(registered) => {
            for warning in &registered.warnings {
                eprintln!("{}", json!({"warning": warning, "url": registered.url}));
            }
            (
                json!({
                    "status": "registered",
                    "id": registered.id,
                    "url": registered.url,
                    "warnings": registered.warnings,
                }),
                ExitCode::SUCCESS,
            )
        }
        Err(failure) => failed(&failure.code),
    }
}

/// The report and exit code of `vapid generate`, for embedding and tests.
pub fn vapid_generate() -> (Value, ExitCode) {
    match merezha_notifications::push::generate() {
        Ok(keys) => (
            json!({
                "status": "generated",
                "private_key": keys.private_key,
                "private_key_pem": keys.private_key_pem,
                "public_key": keys.public_key,
                "key_id": keys.key_id,
            }),
            ExitCode::SUCCESS,
        ),
        Err(failure) => (
            json!({"status": "failed", "code": failure.code}),
            ExitCode::from(1),
        ),
    }
}

/// Log `notification.purge_failed` at error through the bootstrap
/// dispatcher (the command installs no subscriber).
fn report_purge_failure(code: &str, usage: bool) {
    tracing::dispatcher::with_default(&merezha_telemetry::pipeline::bootstrap(), || {
        if usage {
            merezha_telemetry::event!(
                ERROR,
                "notification.purge_failed",
                code,
                usage = PURGE_USAGE
            )
        } else {
            merezha_telemetry::event!(ERROR, "notification.purge_failed", code)
        }
    });
}

/// The command's own arguments: the organization and `--force`, the rest
/// as `merezha run` takes them.
fn purge_arguments(
    args: impl IntoIterator<Item = String>,
) -> Result<(String, bool, crate::run::RunArguments), merezha_domain::Failure> {
    let invalid = || merezha_domain::Failure::permanent("invalid_arguments");
    let mut organization = None;
    let mut force = false;
    let mut rest = Vec::new();
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--force" if !force => force = true,
            "--config" | "--set" => {
                rest.push(argument);
                rest.push(args.next().ok_or_else(invalid)?);
            }
            other if !other.starts_with('-') && organization.is_none() => {
                organization = Some(argument);
            }
            _ => return Err(invalid()),
        }
    }
    Ok((
        organization.ok_or_else(invalid)?,
        force,
        crate::run::arguments(rest)?,
    ))
}

/// Exit codes: 0 when the organization's notification data is purged (now
/// or before), 1 when identity still has the organization (or cannot say)
/// and `--force` is absent, 2 when the arguments, the configuration or the
/// database fail.
fn purge_main(args: impl Iterator<Item = String>) -> ExitCode {
    let (organization, force, arguments) = match purge_arguments(args) {
        Ok(parsed) => parsed,
        Err(failure) => {
            report_purge_failure(&failure.code, true);
            return ExitCode::from(2);
        }
    };
    let input = ConfigInput {
        path: arguments.config,
        env: std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect(),
        overrides: arguments.overrides,
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            report_purge_failure("internal", false);
            return ExitCode::from(2);
        }
    };
    match runtime.block_on(purge(&input, &organization, force)) {
        Ok((text, code)) => {
            print!("{text}");
            ExitCode::from(code)
        }
        Err(code) => ExitCode::from(code),
    }
}

/// The report and its exit code, or the exit code after logging why the
/// purge could not run.
pub async fn purge(
    input: &ConfigInput,
    organization: &str,
    force: bool,
) -> Result<(String, u8), u8> {
    use crate::content::{InIdentity, in_identity};
    use merezha_notifications::tenants;
    use std::{fmt::Write as _, time::Instant};
    let blocks = crate::blocks(input, merezha_content::ContentModule::new());
    let config = match host::configure(input, blocks) {
        Ok((config, _)) => config,
        Err(_) => {
            report_purge_failure("invalid_config", false);
            return Err(2);
        }
    };
    if !config.modules.contains_key(merezha_notifications::NAME) {
        report_purge_failure("notifications_not_enabled", false);
        return Err(2);
    }
    if !tenants::valid_organization(organization) {
        report_purge_failure("invalid_organization", true);
        return Err(2);
    }
    let unavailable = |failure: merezha_domain::Failure| {
        report_purge_failure(&failure.code, false);
        2
    };
    let database = merezha_platform::Database::connect(&config.database).map_err(unavailable)?;
    let deadline =
        Instant::now() + config.database.connect_timeout + std::time::Duration::from_secs(10);
    let result = async {
        if !tenants::deletion_ready(&database, deadline)
            .await
            .map_err(unavailable)?
        {
            report_purge_failure("notifications_migration_pending", false);
            return Err(2);
        }
        let mut out = String::new();
        let mut forced = false;
        match in_identity(&database, organization, deadline)
            .await
            .map_err(unavailable)?
        {
            InIdentity::Deleted => {}
            InIdentity::Live if !force => {
                let _ = writeln!(
                    out,
                    "refused: identity still has organization {organization}; delete it there \
                     (its notification data then purges on its own) or pass --force"
                );
                return Ok((out, 1));
            }
            InIdentity::Unknown(why) if !force => {
                let _ = writeln!(
                    out,
                    "refused: identity cannot confirm organization {organization} is deleted \
                     ({why}); pass --force to purge it anyway"
                );
                return Ok((out, 1));
            }
            InIdentity::Live => {
                forced = true;
                let _ = writeln!(
                    out,
                    "warning: --force: identity still has organization {organization}; its \
                     notification data is purged for good and the block refuses the \
                     organization from now on, while its members keep it in identity"
                );
            }
            InIdentity::Unknown(why) => {
                forced = true;
                let _ = writeln!(
                    out,
                    "warning: --force: identity cannot confirm organization {organization} is \
                     deleted ({why}); its notification data is purged for good and the block \
                     refuses the organization from now on"
                );
            }
        }
        let report = tenants::purge_organization(
            &database,
            organization,
            tenants::DeletedBy::Operator,
            PURGE_STEP_TIMEOUT,
        )
        .await
        .map_err(unavailable)?;
        let source = tenants::DeletedBy::Operator.as_str();
        let purged = report.purged;
        tracing::dispatcher::with_default(&merezha_telemetry::pipeline::bootstrap(), || {
            if report.marked {
                merezha_telemetry::event!(
                    INFO,
                    "notification.organization_deleted",
                    tenant = organization,
                    source,
                    forced
                );
            }
            if !report.already_purged {
                merezha_telemetry::event!(
                    INFO,
                    "notification.organization_purged",
                    tenant = organization,
                    source,
                    forced,
                    webhook_events = purged.webhook_events,
                    webhook_endpoints = purged.webhook_endpoints,
                    push_subscriptions = purged.push_subscriptions,
                    inbox_items = purged.inbox_items,
                    notifications = purged.notifications
                );
            }
        });
        let _ = writeln!(
            out,
            "organization {organization} {}",
            if report.marked {
                "marked deleted"
            } else {
                "already marked deleted"
            }
        );
        let _ = writeln!(
            out,
            "purged webhook_events {} webhook_endpoints {} push_subscriptions {} inbox_items {} \
             notifications {}",
            purged.webhook_events,
            purged.webhook_endpoints,
            purged.push_subscriptions,
            purged.inbox_items,
            purged.notifications
        );
        let _ = writeln!(
            out,
            "status purged{}",
            if report.already_purged {
                " (already)"
            } else {
                ""
            }
        );
        Ok((out, 0))
    }
    .await;
    database.close();
    result
}
