//! `merezha content diff --config merezha.toml`: the classified diff of the
//! model file against the configured database's active model, without
//! changing anything (content design section 2). Projects run it in CI.
use crate::run::{RunArguments, arguments};
use merezha_content::{
    ChangeClass, ContentModule, Plan, Status, StoredModel, derived_indexes, existing_indexes,
    missing_indexes, read_active, unneeded_indexes,
};
use merezha_domain::Failure;
use merezha_platform::{ConfigInput, Database, host};
use serde_json::json;
use std::{
    fmt::Write as _,
    process::ExitCode,
    time::{Duration, Instant},
};

pub const USAGE: &str = "merezha content diff --config PATH [--set SECTION.KEY=VALUE]...";

/// Exit codes: 0 when the model is current or would activate, 1 when it
/// would not (refused changes, a reused version, or a file older than the
/// active model), 2 when the configuration, the model file or the database
/// could not be read.
pub async fn main(args: impl IntoIterator<Item = String>) -> ExitCode {
    let mut args = args.into_iter();
    let parsed = match args.next().as_deref() {
        Some("diff") => arguments(args),
        _ => Err(Failure::permanent("invalid_arguments")),
    };
    let RunArguments { config, overrides } = match parsed {
        Ok(parsed) => parsed,
        Err(failure) => {
            crate::emit(
                "content.diff_failed",
                json!({"code": failure.code, "usage": USAGE}),
            );
            return ExitCode::from(2);
        }
    };
    let input = ConfigInput {
        path: config,
        env: std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect(),
        overrides,
    };
    match diff(&input).await {
        Ok((text, code)) => {
            print!("{text}");
            ExitCode::from(code)
        }
        Err(code) => ExitCode::from(code),
    }
}

/// The report and its exit code, or the exit code after logging why the
/// diff could not run.
pub async fn diff(input: &ConfigInput) -> Result<(String, u8), u8> {
    let module = ContentModule::new();
    let handle = module.handle();
    // Every block the binary registers, so their sections are not unknown.
    let config = match host::configure(input, crate::blocks(input, module)) {
        Ok((config, _)) => config,
        Err(errors) => {
            let issues: Vec<_> = errors
                .0
                .iter()
                .map(|i| json!({"key": i.key, "code": i.code, "origin": i.origin.to_string()}))
                .collect();
            crate::emit(
                "content.diff_failed",
                json!({"code": "invalid_config", "issues": issues}),
            );
            return Err(2);
        }
    };
    let Some(state) = handle.state() else {
        crate::emit(
            "content.diff_failed",
            json!({"code": "content_not_enabled"}),
        );
        return Err(2);
    };
    let unavailable = |failure: Failure| {
        crate::emit("content.diff_failed", json!({"code": failure.code}));
        2
    };
    let database = Database::connect(&config.database).map_err(unavailable)?;
    let deadline = Instant::now() + config.database.connect_timeout + Duration::from_secs(10);
    let (stored, existing) = {
        let mut client = database.acquire(deadline).await.map_err(unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|e| unavailable(merezha_postgres::classify(&e)))?;
        transaction
            .batch_execute("SET TRANSACTION READ ONLY")
            .await
            .map_err(|e| unavailable(merezha_postgres::classify(&e)))?;
        let stored = read_active(&*transaction).await.map_err(unavailable)?;
        let existing = if stored == StoredModel::NoSchema {
            Default::default()
        } else {
            existing_indexes(&*transaction).await.map_err(unavailable)?
        };
        (stored, existing)
    };
    database.close();
    let model = state.model();
    let plan = Plan::new(stored.active(), model);
    let mut out = String::new();
    let _ = writeln!(
        out,
        "model {} version {} fingerprint {}",
        state.path().display(),
        model.version(),
        model.fingerprint()
    );
    match stored.active() {
        Some(active) => {
            let _ = writeln!(
                out,
                "active version {} fingerprint {}",
                active.version, active.fingerprint
            );
        }
        None => out.push_str("active none\n"),
    }
    let _ = writeln!(out, "status {}", plan.status.describe());
    for change in &plan.changes {
        let _ = writeln!(out, "{change}");
    }
    if !matches!(plan.status, Status::Outdated) {
        let expected = derived_indexes(model);
        for index in missing_indexes(&expected, &existing) {
            let _ = writeln!(
                out,
                "index build {} {}.{} {} on {}",
                index.name,
                index.content_type,
                index.field,
                index.purpose.as_str(),
                index.table.as_str()
            );
        }
        for name in unneeded_indexes(&expected, &existing) {
            let _ = writeln!(out, "index drop {name}");
        }
    }
    let refused = plan.changes.iter().any(|c| c.class == ChangeClass::Refused);
    let code = match plan.status {
        Status::Fresh | Status::Current | Status::Pending if !refused => 0,
        _ => 1,
    };
    Ok((out, code))
}
