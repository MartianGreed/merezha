//! `merezha notifications check --config merezha.toml`: the notifications
//! block's startup validation (NT §4, §7.2) without starting anything. It
//! loads the project like `merezha run`, kernel sections included, and
//! prints one JSON report on stdout: `{"status":"ok",...}` and exit 0, or
//! `{"status":"invalid","issues":[{"key","code","origin"}]}` and exit 2.
//! An ok report lists `warnings` (`[{key, code}]`), such as
//! `security_mail_disabled` with `email.driver = "disabled"`. Issues and
//! warnings name settings and template files, never values.
use merezha_notifications::{HostFacts, Notifications};
use merezha_platform::{ConfigInput, Module, host};
use serde_json::{Value, json};
use std::process::ExitCode;

pub const USAGE: &str =
    "merezha notifications check --config PATH [--listen ADDRESS] [--set SECTION.KEY=VALUE]...";

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
    if args.next().as_deref() != Some("check") {
        return invalid();
    }
    let Ok(arguments) = crate::run::arguments(args) else {
        return invalid();
    };
    let input = ConfigInput {
        path: arguments.config,
        env: std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect(),
        overrides: arguments.overrides,
    };
    let (report, code) = check(&input);
    println!("{report}");
    code
}
