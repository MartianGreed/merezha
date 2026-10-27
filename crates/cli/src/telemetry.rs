//! `merezha telemetry export --out DIR` (TE §7, TE-T7): writes the Grafana
//! dashboards, their provisioning file and the Prometheus rule files this
//! host embeds ([`merezha_telemetry::artifacts`]) into `DIR`, and prints one
//! JSON report on stdout.
use merezha_telemetry::artifacts::{self, ExportError};
use serde_json::{Value, json};
use std::{io::ErrorKind, path::PathBuf, process::ExitCode};

pub const USAGE: &str = "merezha telemetry export --out DIR";

/// The export directory of `export --out DIR`, or `None` for anything else.
pub fn arguments(args: impl IntoIterator<Item = String>) -> Option<PathBuf> {
    let mut args = args.into_iter();
    if args.next().as_deref() != Some("export") {
        return None;
    }
    let mut out = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--out" if out.is_none() => {
                let value = args.next()?;
                if value.starts_with("--") {
                    return None;
                }
                out = Some(PathBuf::from(value));
            }
            _ => return None,
        }
    }
    out.filter(|dir| !dir.as_os_str().is_empty())
}

/// Export into `out`: exit 0 with the files written, 1 when a file cannot be
/// written (the report names it, the I/O error kind, never the OS text, and
/// the files already replaced).
pub fn export(out: &std::path::Path) -> (Value, ExitCode) {
    match artifacts::export(out) {
        Ok(files) => (
            json!({
                "status": "exported",
                "out": out.display().to_string(),
                "host_version": env!("CARGO_PKG_VERSION"),
                "files": files,
            }),
            ExitCode::SUCCESS,
        ),
        Err(ExportError {
            path,
            kind,
            written,
        }) => (
            json!({
                "status": "failed",
                "error": {"code": "export_failed", "reason": reason(kind), "file": path},
                "files": written,
            }),
            ExitCode::from(1),
        ),
    }
}

/// The I/O error kinds a report names; any other is `io_error`. These are
/// report reasons, not error codes of the kernel registry.
const REASONS: [(ErrorKind, &str); 7] = [
    (ErrorKind::PermissionDenied, "permission_denied"),
    (ErrorKind::NotFound, "not_found"),
    (ErrorKind::AlreadyExists, "already_exists"),
    (ErrorKind::NotADirectory, "not_a_directory"),
    (ErrorKind::IsADirectory, "is_a_directory"),
    (ErrorKind::ReadOnlyFilesystem, "read_only_filesystem"),
    (ErrorKind::StorageFull, "storage_full"),
];

fn reason(kind: ErrorKind) -> &'static str {
    REASONS
        .iter()
        .find(|(k, _)| *k == kind)
        .map_or("io_error", |(_, reason)| reason)
}

pub fn main(args: impl IntoIterator<Item = String>) -> ExitCode {
    let (value, code) = match arguments(args) {
        Some(out) => export(&out),
        None => (
            json!({"status": "invalid", "error": {"code": "invalid_arguments"}, "usage": USAGE}),
            ExitCode::from(2),
        ),
    };
    println!("{value}");
    code
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Option<PathBuf> {
        arguments(args.iter().map(|a| a.to_string()))
    }

    #[test]
    fn arguments_are_strict() {
        assert_eq!(
            parse(&["export", "--out", "ops/telemetry"]),
            Some(PathBuf::from("ops/telemetry"))
        );
        for bad in [
            &[][..],
            &["export"][..],
            &["export", "--out"][..],
            &["export", "--out", ""][..],
            &["export", "--out", "a", "--out", "b"][..],
            &["export", "--out", "a", "--force"][..],
            &["export", "--out", "--force"][..],
            &["export", "--out", "--out", "a"][..],
            &["import", "--out", "a"][..],
        ] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
    }
}
