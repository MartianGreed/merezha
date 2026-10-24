//! `merezha application verify --config merezha.toml`: read-only checks of
//! the PostgreSQL application schema (AP §A.11). It prints one JSON report
//! on stdout and exits 0 when every invariant holds, 1 on any violation and
//! 2 when it could not check (arguments, configuration, database, schema),
//! with `{"ok":false,"error":{code,classification}}` instead of a report.
use merezha_domain::Failure;
use merezha_platform::{ConfigInput, Database, ProjectConfig};
use serde_json::json;
use std::{
    path::PathBuf,
    process::ExitCode,
    time::{Duration, Instant},
};

pub const USAGE: &str = "merezha application verify --config PATH";
/// Bound of one verification run; it reads the whole history in chunks.
pub const VERIFY_TIMEOUT: Duration = Duration::from_secs(600);

/// Parse the arguments after `application`.
pub fn arguments(args: impl IntoIterator<Item = String>) -> Result<PathBuf, Failure> {
    let invalid = || Failure::permanent("invalid_arguments");
    let mut args = args.into_iter();
    if args.next().as_deref() != Some("verify") || args.next().as_deref() != Some("--config") {
        return Err(invalid());
    }
    let path = PathBuf::from(args.next().ok_or_else(invalid)?);
    if args.next().is_some() {
        return Err(invalid());
    }
    Ok(path)
}

/// Entry point for `merezha application`.
pub async fn main(args: impl IntoIterator<Item = String>) -> ExitCode {
    match verify(args).await {
        Ok(report) => {
            println!("{}", json!(report));
            if report.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(failure) => {
            println!("{}", json!({"ok": false, "error": failure, "usage": USAGE}));
            ExitCode::from(2)
        }
    }
}

async fn verify(
    args: impl IntoIterator<Item = String>,
) -> Result<merezha_application::postgres::VerifyReport, Failure> {
    let path = arguments(args)?;
    let input = ConfigInput {
        path,
        env: std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
            .collect(),
        overrides: Vec::new(),
    };
    // Issues name keys only; the report carries the code.
    let config = ProjectConfig::load(&input).map_err(|_| Failure::permanent("invalid_config"))?;
    let database = Database::connect(&config.database)?;
    let reach = Instant::now() + config.database.connect_timeout + config.database.acquire_timeout;
    database
        .ping(reach)
        .await
        .map_err(|_| Failure::transient("database_unavailable"))?;
    let result =
        merezha_application::postgres::verify(&database, Instant::now() + VERIFY_TIMEOUT).await;
    database.close();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_need_verify_and_one_config() {
        let parse = |args: &[&str]| arguments(args.iter().map(|a| a.to_string()));
        assert_eq!(
            parse(&["verify", "--config", "m.toml"]).unwrap(),
            PathBuf::from("m.toml")
        );
        for bad in [
            &[][..],
            &["verify"][..],
            &["check", "--config", "m.toml"][..],
            &["verify", "--config"][..],
            &["verify", "--config", "a", "b"][..],
        ] {
            assert_eq!(parse(bad).unwrap_err().code, "invalid_arguments", "{bad:?}");
        }
    }
}
