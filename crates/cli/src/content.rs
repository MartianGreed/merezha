//! `merezha content diff --config merezha.toml`: the classified diff of the
//! model file against the configured database's active model, without
//! changing anything (content design section 2). Projects run it in CI.
//!
//! `merezha content purge-organization <organization> --config
//! merezha.toml [--force]` (issue #56): mark an organization's content
//! scope deleted and purge it, for organizations identity deleted before
//! content subscribed to `identity.organization_deleted`, or a missed
//! event. It refuses an organization identity still has, unless `--force`.
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
/// The usage of `merezha content purge-organization`.
pub const PURGE_USAGE: &str = "merezha content purge-organization ORGANIZATION --config PATH [--force] [--set SECTION.KEY=VALUE]...";
/// Bound on each purge transaction of the command.
const PURGE_STEP_TIMEOUT: Duration = Duration::from_secs(60);

/// What `content.diff_failed` carries besides its code.
enum Detail {
    None,
    Usage,
    Issues(String),
}
/// Log `content.diff_failed` at error: JSON to stderr through the bootstrap
/// dispatcher (the command installs no subscriber).
fn report_diff_failure(code: &str, detail: Detail) {
    tracing::dispatcher::with_default(&merezha_telemetry::pipeline::bootstrap(), || match detail {
        Detail::Issues(issues) => {
            merezha_telemetry::event!(ERROR, "content.diff_failed", code, issues = issues.as_str())
        }
        Detail::Usage => {
            merezha_telemetry::event!(ERROR, "content.diff_failed", code, usage = USAGE)
        }
        Detail::None => merezha_telemetry::event!(ERROR, "content.diff_failed", code),
    });
}

/// Exit codes: 0 when the model is current or would activate, 1 when it
/// would not (refused changes, a reused version, or a file older than the
/// active model), 2 when the configuration, the model file or the database
/// could not be read.
pub async fn main(args: impl IntoIterator<Item = String>) -> ExitCode {
    let mut args = args.into_iter();
    let parsed = match args.next().as_deref() {
        Some("diff") => arguments(args),
        Some("purge-organization") => return purge_main(args).await,
        _ => Err(Failure::permanent("invalid_arguments")),
    };
    let RunArguments { config, overrides } = match parsed {
        Ok(parsed) => parsed,
        Err(failure) => {
            report_diff_failure(&failure.code, Detail::Usage);
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
            // The model's own messages name rules and TOML paths, never
            // values: print them as `merezha content diff` promises.
            if let Some(error) = handle.model_error() {
                println!("{error}");
            }
            let issues: Vec<_> = errors
                .0
                .iter()
                .map(|i| json!({"key": i.key, "code": i.code, "origin": i.origin.to_string()}))
                .collect();
            report_diff_failure(
                "invalid_config",
                Detail::Issues(serde_json::Value::from(issues).to_string()),
            );
            return Err(2);
        }
    };
    let Some(state) = handle.state() else {
        report_diff_failure("content_not_enabled", Detail::None);
        return Err(2);
    };
    let unavailable = |failure: Failure| {
        report_diff_failure(&failure.code, Detail::None);
        2
    };
    let database = Database::connect(&config.database).map_err(unavailable)?;
    let deadline = Instant::now() + config.database.connect_timeout + Duration::from_secs(10);
    let (stored, existing, rebuild) = {
        let mut client = database.acquire(deadline).await.map_err(unavailable)?;
        let transaction = client
            .transaction()
            .await
            .map_err(|e| unavailable(merezha_postgres::classify(&e)))?;
        database
            .batch(&*transaction, &merezha_content::DIFF_READ_ONLY)
            .await
            .map_err(|e| unavailable(merezha_postgres::classify(&e)))?;
        let stored = read_active(&database, &*transaction)
            .await
            .map_err(unavailable)?;
        let existing = if stored == StoredModel::NoSchema {
            Default::default()
        } else {
            existing_indexes(&database, &*transaction)
                .await
                .map_err(unavailable)?
        };
        let rebuild = merezha_content::pending_rebuild(&database, &*transaction)
            .await
            .map_err(unavailable)?;
        (stored, existing, rebuild)
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
    // Informational: the background job finishes it; it never fails the diff.
    if let Some(rebuild) = rebuild {
        let _ = writeln!(
            out,
            "content_rebuild_pending version {} after {}",
            rebuild.version,
            rebuild.after_entry.as_deref().unwrap_or("start")
        );
    }
    let refused = plan.changes.iter().any(|c| c.class == ChangeClass::Refused);
    let code = match plan.status {
        Status::Fresh | Status::Current | Status::Pending if !refused => 0,
        _ => 1,
    };
    Ok((out, code))
}

/// Log `content.purge_failed` at error through the bootstrap dispatcher.
fn report_purge_failure(code: &str, usage: bool) {
    tracing::dispatcher::with_default(&merezha_telemetry::pipeline::bootstrap(), || {
        if usage {
            merezha_telemetry::event!(ERROR, "content.purge_failed", code, usage = PURGE_USAGE)
        } else {
            merezha_telemetry::event!(ERROR, "content.purge_failed", code)
        }
    });
}

/// `content.organization_deleted` (when this run marked the scope) and
/// `content.organization_purged`, source `operator`, on stderr through the
/// bootstrap dispatcher, `forced` when `--force` overrode identity.
fn log_purge(organization: &str, report: &merezha_content::PurgeReport, forced: bool) {
    tracing::dispatcher::with_default(&merezha_telemetry::pipeline::bootstrap(), || {
        if report.marked {
            merezha_telemetry::event!(
                INFO,
                "content.organization_deleted",
                tenant = organization,
                source = merezha_content::DeletedBy::Operator.as_str(),
                forced
            );
        }
        if !report.already_purged {
            merezha_telemetry::event!(
                INFO,
                "content.organization_purged",
                tenant = organization,
                entries = report.entries,
                assets = report.assets,
                blobs = report.blobs,
                forced
            );
        }
    });
}

/// The command's own arguments: the organization and `--force`, the rest
/// as `merezha run` takes them.
fn purge_arguments(
    args: impl IntoIterator<Item = String>,
) -> Result<(String, bool, RunArguments), Failure> {
    let invalid = || Failure::permanent("invalid_arguments");
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
    Ok((organization.ok_or_else(invalid)?, force, arguments(rest)?))
}

/// Whether identity still holds `organization`.
enum InIdentity {
    /// Its row is there: identity has not deleted it.
    Live,
    /// No row: deleted (or never created) in identity.
    Deleted,
    /// Identity cannot say: no identity schema in the database, or an id
    /// identity never issues.
    Unknown(&'static str),
}

async fn in_identity(
    database: &Database,
    organization: &str,
    deadline: Instant,
) -> Result<InIdentity, Failure> {
    let Ok(id) = merezha_identity::ids::parse(merezha_identity::ids::ORGANIZATION, organization)
    else {
        return Ok(InIdentity::Unknown("not an identity organization id"));
    };
    let client = database.acquire(deadline).await?;
    match database
        .query_opt(&**client, &merezha_identity::sql::ORG_GET, &[&id])
        .await
    {
        Ok(Some(_)) => Ok(InIdentity::Live),
        Ok(None) => Ok(InIdentity::Deleted),
        Err(error)
            if error.code()
                == Some(&merezha_postgres::tokio_postgres::error::SqlState::UNDEFINED_TABLE) =>
        {
            Ok(InIdentity::Unknown(
                "the database has no identity organizations",
            ))
        }
        Err(error) => Err(merezha_postgres::classify(&error)),
    }
}

/// Exit codes: 0 when the organization's content is purged (now or
/// before), 1 when identity still has the organization (or cannot say)
/// and `--force` is absent, 2 when the arguments, the configuration or
/// the database fail.
async fn purge_main(args: impl IntoIterator<Item = String>) -> ExitCode {
    let (organization, force, RunArguments { config, overrides }) = match purge_arguments(args) {
        Ok(parsed) => parsed,
        Err(failure) => {
            report_purge_failure(&failure.code, true);
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
    match purge(&input, &organization, force).await {
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
    let module = ContentModule::new();
    let handle = module.handle();
    let config = match host::configure(input, crate::blocks(input, module)) {
        Ok((config, _)) => config,
        Err(_) => {
            report_purge_failure("invalid_config", false);
            return Err(2);
        }
    };
    let Some(state) = handle.state() else {
        report_purge_failure("content_not_enabled", false);
        return Err(2);
    };
    let scope = match merezha_content::organization_scope(organization) {
        Ok(scope) => scope,
        Err(failure) => {
            report_purge_failure(&failure.code, true);
            return Err(2);
        }
    };
    let unavailable = |failure: Failure| {
        report_purge_failure(&failure.code, false);
        2
    };
    let database = Database::connect(&config.database).map_err(unavailable)?;
    let deadline = Instant::now() + config.database.connect_timeout + Duration::from_secs(10);
    let result = async {
        if !merezha_content::deletion_ready(&database, deadline)
            .await
            .map_err(unavailable)?
        {
            report_purge_failure("content_migration_pending", false);
            return Err(2);
        }
        let mut out = String::new();
        // `--force` overrode identity's answer.
        let mut force_used = false;
        match in_identity(&database, organization, deadline)
            .await
            .map_err(unavailable)?
        {
            InIdentity::Deleted => {}
            InIdentity::Live if !force => {
                let _ = writeln!(
                    out,
                    "refused: identity still has organization {organization}; delete it there \
                     (its content then purges on its own) or pass --force"
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
                force_used = true;
                let _ = writeln!(
                    out,
                    "warning: --force: identity still has organization {organization}; its \
                     content is purged for good and content refuses the organization from now \
                     on, while its members keep it in identity"
                );
            }
            InIdentity::Unknown(why) => {
                force_used = true;
                let _ = writeln!(
                    out,
                    "warning: --force: identity cannot confirm organization {organization} is \
                     deleted ({why}); its content is purged for good and content refuses the \
                     organization from now on"
                );
            }
        }
        // The released blobs go at the next `content.blob_cleanup`, which a
        // host with media runs at once when the purge appends it.
        let cleanup = match state.media() {
            Some(_) => Some(merezha_platform::ports::SourceOutbox::new(
                merezha_platform::ports::Source::new(merezha_content::NAME).map_err(unavailable)?,
                std::sync::Arc::new(merezha_jobs::PgOutbox),
            )),
            None => None,
        };
        let report = merezha_content::purge_organization(
            &database,
            &scope,
            merezha_content::DeletedBy::Operator,
            cleanup.as_ref(),
            PURGE_STEP_TIMEOUT,
            |_| {},
        )
        .await
        .map_err(unavailable)?;
        log_purge(organization, &report, force_used);
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
            "purged entries {} assets {} blobs {}",
            report.entries, report.assets, report.blobs
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
        if cleanup.is_some() && report.blobs > 0 {
            out.push_str("blobs released to content.blob_cleanup\n");
        }
        Ok((out, 0))
    }
    .await;
    database.close();
    result
}
