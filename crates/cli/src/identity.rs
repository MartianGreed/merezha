//! `merezha identity password-benchmark`: time Argon2id on this machine
//! (identity section 7), with the parameters of `[modules.identity.password]`
//! when `--config` names a project, or the defaults, each overridable.
use merezha_identity::{
    benchmark::{self, MAX_SAMPLES},
    config::PasswordSettings,
    hasher::Cost,
};
use merezha_platform::{ConfigInput, host};
use serde_json::{Value, json};
use std::{path::PathBuf, process::ExitCode};

pub const USAGE: &str = "merezha identity password-benchmark [--config PATH] [--set SECTION.KEY=VALUE]... \
     [--memory-kib N] [--iterations N] [--parallelism N] [--samples N]";

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
            if let Ok(settings) = merezha_identity::Settings::parse(&section, facts.as_ref())
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

/// Entry point for `merezha identity ...`; prints one JSON object.
pub fn main(args: impl IntoIterator<Item = String>) -> ExitCode {
    let mut args = args.into_iter();
    let parsed = match args.next().as_deref() {
        Some("password-benchmark") => arguments(args),
        _ => None,
    };
    let (value, code) = match parsed {
        Some(parsed) => report(
            &parsed,
            std::env::vars_os()
                .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)))
                .collect(),
        ),
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
