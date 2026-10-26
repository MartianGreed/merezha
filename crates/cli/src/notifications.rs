//! `merezha notifications check --config merezha.toml`: the notifications
//! block's startup validation (NT §4, §7.2) without starting anything. It
//! loads the project like `merezha run`, kernel sections included, and
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
use merezha_notifications::{HostFacts, Notifications};
use merezha_platform::{ConfigInput, Module, host};
use serde_json::{Value, json};
use std::process::ExitCode;

pub const USAGE: &str = "merezha notifications check --config PATH [--listen ADDRESS] \
     [--set SECTION.KEY=VALUE]... | merezha notifications brevo register-webhook --config PATH \
     [--url URL [--allow-host]] [--set SECTION.KEY=VALUE]... | merezha notifications vapid generate";

/// How long `brevo register-webhook` waits for Brevo.
const REGISTER_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// The report and exit code of a check, for embedding and tests.
pub fn check(input: &ConfigInput) -> (Value, ExitCode) {
    let module = Notifications::new(HostFacts::load(input));
    let handle = module.handle();
    let modules: Vec<Box<dyn Module>> = vec![Box::new(module)];
    match host::configure(input, modules) {
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
    if let Err(errors) = host::configure(input, vec![Box::new(module) as Box<dyn Module>]) {
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
