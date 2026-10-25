//! `merezha identity password-benchmark`: time Argon2id on this machine
//! (identity section 7), with the parameters of `[modules.identity.password]`
//! when `--config` names a project, or the defaults, each overridable.
//!
//! `merezha identity platform-admin grant|revoke <email> --config PATH`:
//! the only way to grant the break-glass `platform_admin` role (identity
//! section 5), audited `platform_admin.granted` / `platform_admin.revoked`.
use merezha_identity::{
    benchmark::{self, MAX_SAMPLES},
    config::PasswordSettings,
    hasher::Cost,
};
use merezha_platform::{ConfigInput, host};
use serde_json::{Value, json};
use std::{path::PathBuf, process::ExitCode};

pub const USAGE: &str = "merezha identity password-benchmark [--config PATH] [--set SECTION.KEY=VALUE]... \
     [--memory-kib N] [--iterations N] [--parallelism N] [--samples N] | \
     merezha identity platform-admin grant|revoke EMAIL --config PATH [--set SECTION.KEY=VALUE]...";

/// Parsed arguments.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BenchmarkArguments {
    pub config: Option<PathBuf>,
    pub overrides: Vec<(String, String)>,
    pub memory_kib: Option<u32>,
    pub iterations: Option<u32>,
    pub parallelism: Option<u32>,
    pub samples: u32,
}

/// Parse the arguments after `password-benchmark`.
pub fn arguments(args: impl IntoIterator<Item = String>) -> Option<BenchmarkArguments> {
    let mut parsed = BenchmarkArguments {
        samples: 5,
        ..BenchmarkArguments::default()
    };
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        let mut value = || args.next();
        match argument.as_str() {
            "--config" if parsed.config.is_none() => parsed.config = Some(PathBuf::from(value()?)),
            "--set" => {
                let pair = value()?;
                let (key, value) = pair.split_once('=')?;
                if key.is_empty() || parsed.overrides.len() >= 64 {
                    return None;
                }
                parsed.overrides.push((key.to_owned(), value.to_owned()));
            }
            "--memory-kib" => parsed.memory_kib = Some(value()?.parse().ok()?),
            "--iterations" => parsed.iterations = Some(value()?.parse().ok()?),
            "--parallelism" => parsed.parallelism = Some(value()?.parse().ok()?),
            "--samples" => parsed.samples = value()?.parse().ok()?,
            _ => return None,
        }
    }
    (parsed.samples >= 1
        && parsed.samples <= MAX_SAMPLES
        && (parsed.config.is_some() || parsed.overrides.is_empty()))
    .then_some(parsed)
}

fn invalid(code: &str) -> (Value, ExitCode) {
    (
        json!({"status": "invalid", "error": {"code": code}}),
        ExitCode::from(2),
    )
}

/// The report and exit code: 0 with the timings, 2 for invalid arguments,
/// configuration or parameters.
pub fn report(parsed: &BenchmarkArguments, env: Vec<(String, String)>) -> (Value, ExitCode) {
    let mut cost = Cost::of(&PasswordSettings::default());
    if let Some(path) = &parsed.config {
        let input = ConfigInput {
            path: path.clone(),
            env,
            overrides: parsed.overrides.clone(),
        };
        let modules = crate::blocks(&input, merezha_content::ContentModule::new());
        let identity_settings = match host::configure(&input, modules) {
            Ok((config, _)) => config.modules.get(merezha_identity::NAME).cloned(),
            Err(errors) => {
                let issues: Vec<Value> = errors
                    .0
                    .iter()
                    .map(|i| json!({"key": i.key, "code": i.code, "origin": i.origin.to_string()}))
                    .collect();
                return (
                    json!({"status": "invalid", "error": {"code": "invalid_config"}, "issues": issues}),
                    ExitCode::from(2),
                );
            }
        };
        if let Some(section) = identity_settings {
            let facts = merezha_identity::HostFacts::load(&input);
            let catalog = crate::identity_module(&input).catalog().clone();
            if let Ok(settings) =
                merezha_identity::Settings::parse(&section, facts.as_ref(), &catalog)
                && let Some(password) = &settings.password
            {
                cost = Cost::of(password);
            }
        }
    }
    if let Some(m) = parsed.memory_kib {
        cost.memory_kib = m;
    }
    if let Some(t) = parsed.iterations {
        cost.iterations = t;
    }
    if let Some(p) = parsed.parallelism {
        cost.parallelism = p;
    }
    if cost.memory_kib < PasswordSettings::MIN_MEMORY_KIB
        || cost.iterations < PasswordSettings::MIN_ITERATIONS
        || cost.parallelism == 0
    {
        return invalid("out_of_range");
    }
    match benchmark::run(cost, parsed.samples) {
        Ok(report) => (report.json(), ExitCode::SUCCESS),
        Err(failure) => invalid(&failure.code),
    }
}

/// Parsed `platform-admin` arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminArguments {
    pub grant: bool,
    pub email: String,
    pub config: PathBuf,
    pub overrides: Vec<(String, String)>,
}
/// Parse the arguments after `platform-admin`.
pub fn admin_arguments(args: impl IntoIterator<Item = String>) -> Option<AdminArguments> {
    let mut args = args.into_iter();
    let grant = match args.next()?.as_str() {
        "grant" => true,
        "revoke" => false,
        _ => return None,
    };
    let mut email = None;
    let mut config = None;
    let mut overrides = Vec::new();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--config" if config.is_none() => config = Some(PathBuf::from(args.next()?)),
            "--set" => {
                let pair = args.next()?;
                let (key, value) = pair.split_once('=')?;
                if key.is_empty() || overrides.len() >= 64 {
                    return None;
                }
                overrides.push((key.to_owned(), value.to_owned()));
            }
            text if email.is_none() && !text.starts_with("--") => email = Some(text.to_owned()),
            _ => return None,
        }
    }
    Some(AdminArguments {
        grant,
        email: email?,
        config: config?,
        overrides,
    })
}

/// Run `platform-admin`: the report and its exit code, 0 when done
/// (`granted`, `revoked` or `unchanged`), 2 for invalid arguments,
/// configuration, an unknown or unverified account or pending migrations,
/// 1 when the database failed.
pub async fn platform_admin(
    parsed: &AdminArguments,
    env: Vec<(String, String)>,
) -> (Value, ExitCode) {
    let Ok(email) = merezha_identity::normalize::email(&parsed.email) else {
        return invalid("invalid_email");
    };
    let input = ConfigInput {
        path: parsed.config.clone(),
        env,
        overrides: parsed.overrides.clone(),
    };
    let config = match host::configure(&input, crate::modules(&input)) {
        Ok((config, _)) => config,
        Err(errors) => {
            let issues: Vec<Value> = errors
                .0
                .iter()
                .map(|i| json!({"key": i.key, "code": i.code, "origin": i.origin.to_string()}))
                .collect();
            return (
                json!({"status": "invalid", "error": {"code": "invalid_config"}, "issues": issues}),
                ExitCode::from(2),
            );
        }
    };
    let Some(section) = config.modules.get(merezha_identity::NAME) else {
        return invalid("identity_disabled");
    };
    let mut identity = crate::identity_module(&input);
    if merezha_platform::Module::configure(&mut identity, section).is_err() {
        return invalid("invalid_config");
    }
    let failed = |code: &str| {
        (
            json!({"status": "failed", "error": {"code": code}}),
            ExitCode::from(1),
        )
    };
    let database = match merezha_platform::Database::connect(&config.database) {
        Ok(database) => database,
        Err(failure) => return failed(&failure.code),
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let jobs = merezha_jobs::PgJobs::new(&config.jobs);
    let declared = [
        merezha_postgres::ModuleMigrations {
            module: merezha_identity::NAME,
            migrations: merezha_identity::MIGRATIONS,
        },
        merezha_postgres::ModuleMigrations {
            module: "jobs",
            migrations: merezha_jobs::PgJobs::migrations(),
        },
    ];
    match merezha_postgres::run_migrations(
        &database,
        &declared,
        merezha_postgres::MigrationMode::Verify,
        deadline,
    )
    .await
    {
        Ok(report) if report.is_current() => {}
        Ok(_) => return invalid("migrations_pending"),
        Err(error) if error.failure.classification == merezha_domain::Classification::Transient => {
            return failed(&error.failure.code);
        }
        Err(error) => return invalid(&error.failure.code),
    }
    let ports = merezha_platform::JobsRuntime::ports(&jobs, &database);
    let ctx = match merezha_platform::AppContext::builder(
        &config.app.id,
        database,
        std::sync::Arc::new(merezha_platform::ports::DenyAll),
    )
    .instance_id(&config.app.instance_id)
    .outbox(ports.outbox)
    .jobs(ports.jobs)
    .build()
    {
        Ok(ctx) => ctx,
        Err(failure) => return failed(&failure.code),
    };
    if let Err(failure) = identity.open_admin(ctx).await {
        return failed(&failure.code);
    }
    match identity.handle().platform_admin(&email, parsed.grant).await {
        Ok((change, user)) => (
            json!({
                "status": "ok",
                "action": change.as_str(),
                "user_id": merezha_identity::ids::render(merezha_identity::ids::USER, user),
            }),
            ExitCode::SUCCESS,
        ),
        Err(failure) if failure.classification == merezha_domain::Classification::Transient => {
            failed(&failure.code)
        }
        Err(failure) => invalid(&failure.code),
    }
}

/// Entry point for `merezha identity ...`; prints one JSON object.
pub fn main(args: impl IntoIterator<Item = String>) -> ExitCode {
    let mut args = args.into_iter();
    let env = || -> Vec<(String, String)> {
        std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect()
    };
    let command = args.next();
    if command.as_deref() == Some("platform-admin") {
        let (value, code) = match admin_arguments(args) {
            Some(parsed) => match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime.block_on(platform_admin(&parsed, env())),
                Err(_) => (
                    json!({"status": "failed", "error": {"code": "internal"}}),
                    ExitCode::from(1),
                ),
            },
            None => {
                let (mut value, code) = invalid("invalid_arguments");
                value["usage"] = Value::String(USAGE.into());
                (value, code)
            }
        };
        println!("{value}");
        return code;
    }
    let parsed = match command.as_deref() {
        Some("password-benchmark") => arguments(args),
        _ => None,
    };
    let (value, code) = match parsed {
        Some(parsed) => report(&parsed, env()),
        None => {
            let (mut value, code) = invalid("invalid_arguments");
            value["usage"] = Value::String(USAGE.into());
            (value, code)
        }
    };
    println!("{value}");
    code
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Option<BenchmarkArguments> {
        arguments(args.iter().map(|a| a.to_string()))
    }

    #[test]
    fn arguments_are_strict() {
        let parsed = parse(&["--memory-kib", "19456", "--samples", "2"]).unwrap();
        assert_eq!((parsed.memory_kib, parsed.samples), (Some(19456), 2));
        for bad in [
            &["--samples", "0"][..],
            &["--samples", "101"][..],
            &["--memory-kib", "lots"][..],
            &["--set", "a.b=c"][..],
            &["--unknown"][..],
            &["--config"][..],
        ] {
            assert!(parse(bad).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn platform_admin_arguments_are_strict() {
        let parse = |args: &[&str]| admin_arguments(args.iter().map(|a| a.to_string()));
        let parsed = parse(&["grant", "ada@example.com", "--config", "m.toml"]).unwrap();
        assert!(parsed.grant);
        assert_eq!(parsed.email, "ada@example.com");
        assert!(
            !parse(&["revoke", "ada@example.com", "--config", "m.toml"])
                .unwrap()
                .grant
        );
        for bad in [
            &[][..],
            &["grant"][..],
            &["grant", "ada@example.com"][..],
            &["promote", "ada@example.com", "--config", "m.toml"][..],
            &["grant", "a@x.io", "b@x.io", "--config", "m.toml"][..],
            &["grant", "a@x.io", "--config", "m.toml", "--force"][..],
        ] {
            assert!(parse(bad).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn the_floors_hold_and_timings_are_reported() {
        let mut parsed = parse(&["--memory-kib", "1024"]).unwrap();
        let (value, _) = report(&parsed, Vec::new());
        assert_eq!(value["error"]["code"], "out_of_range");
        parsed.memory_kib = Some(19_456);
        parsed.samples = 1;
        let (value, _) = report(&parsed, Vec::new());
        assert_eq!(value["status"], "ok");
        assert_eq!(value["argon2_iterations"], 2);
        assert!(value["median_ms"].as_f64().unwrap() > 0.0);
    }
}
