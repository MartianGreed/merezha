//! `merezha new NAME [--dir PATH] [--template starter] [--blocks LIST]
//! [--merezha-rev REV | --merezha-path PATH] [--no-secrets]` (scaffold §2):
//! a new project from a template built into this binary. It reads no
//! configuration, emits no telemetry, needs no network and runs no
//! subprocess.
//!
//! It prints one JSON object on stdout: `{"status":"created","dir",
//! "template","blocks","merezha":{"rev"}|{"path"},"secrets","files","next"}`
//! and exit 0, `{"status":"invalid","error":{"code"}}` and exit 2 before
//! anything is written (§2.3), or `{"status":"failed","error":{"code"}}` and
//! exit 1 when writing failed and nothing was left behind (`write_failed`,
//! `rename_failed`, `random_unavailable`, `secret_generation_failed`). `secrets` names the variables
//! written to `.env`, never a value. `--help` prints the templates and
//! blocks as text. See [`merezha_scaffold`].
use base64::{Engine, engine::general_purpose::STANDARD};
use merezha_platform::{OsRandom, Random, context::random_token};
use merezha_scaffold::{Catalog, Command, Context, Created, Error, Pin, SecretKind, SecretSource};
use serde_json::{Value, json};
use std::{ffi::OsString, path::Path, process::ExitCode};

/// Secrets from the operating system CSPRNG (identity and notification
/// keys) and the generator behind `merezha notifications vapid generate`.
pub struct OsSecrets;

impl SecretSource for OsSecrets {
    fn generate(&mut self, kind: SecretKind) -> Result<String, Error> {
        let unavailable = |_| Error::failed("random_unavailable");
        match kind {
            SecretKind::Token => random_token(&OsRandom).map_err(unavailable),
            SecretKind::Key => {
                let mut bytes = [0u8; 32];
                OsRandom.fill(&mut bytes).map_err(unavailable)?;
                Ok(STANDARD.encode(bytes))
            }
            SecretKind::VapidPrivateKey => merezha_notifications::push::generate()
                .map(|keys| keys.private_key)
                .map_err(|_| Error::failed(merezha_scaffold::SECRET_GENERATION_FAILED)),
        }
    }
}

/// The report of a created project.
pub fn created_report(created: &Created) -> Value {
    let merezha = match &created.pin {
        Pin::Rev(rev) => json!({"rev": rev}),
        Pin::Path(path) => json!({"path": path}),
    };
    json!({
        "status": "created",
        "dir": created.dir.to_string_lossy(),
        "template": created.template,
        "blocks": created.blocks,
        "merezha": merezha,
        "secrets": created.secrets,
        "files": created.files,
        "next": created.next,
    })
}

/// The report of a refusal or a failed write.
pub fn error_report(error: &Error) -> Value {
    let mut detail = json!({"code": error.code});
    if let Some(reason) = &error.reason {
        detail["reason"] = json!(reason);
    }
    if error.code == "invalid_arguments" {
        detail["usage"] = json!(merezha_scaffold::USAGE);
    }
    json!({"status": error.status(), "error": detail})
}

/// Runs `merezha new` in `cwd` with `args` (after `new`): the report and
/// the exit code. `--help` gives its text as a JSON string.
pub fn run(
    args: impl IntoIterator<Item = OsString>,
    cwd: &Path,
    secrets: &mut dyn SecretSource,
) -> (Value, ExitCode) {
    let fail = |error: Error| {
        let code = ExitCode::from(error.exit_code());
        (error_report(&error), code)
    };
    let catalog = match Catalog::embedded() {
        Ok(catalog) => catalog,
        Err(error) => return fail(error),
    };
    let request = match Command::parse(args) {
        Ok(Command::Help) => {
            return (
                Value::String(merezha_scaffold::help(&catalog)),
                ExitCode::SUCCESS,
            );
        }
        Ok(Command::New(request)) => request,
        Err(error) => return fail(error),
    };
    let context = Context {
        catalog: &catalog,
        cwd,
        built_from: crate::guest::source_rev(),
        merezha_version: env!("CARGO_PKG_VERSION"),
    };
    match merezha_scaffold::prepare(&request, &context, secrets).and_then(|plan| plan.write()) {
        Ok(created) => (created_report(&created), ExitCode::SUCCESS),
        Err(error) => fail(error),
    }
}

/// Entry point for `merezha new`.
pub fn main(args: impl Iterator<Item = OsString>) -> ExitCode {
    let Ok(cwd) = std::env::current_dir() else {
        println!("{}", error_report(&Error::invalid("parent_missing")));
        return ExitCode::from(2);
    };
    let (report, code) = run(args, &cwd, &mut OsSecrets);
    match report {
        Value::String(help) => print!("{help}"),
        report => println!("{report}"),
    }
    code
}
