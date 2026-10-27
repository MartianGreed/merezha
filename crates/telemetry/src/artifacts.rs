//! The importable telemetry artifacts of `deploy/telemetry/` (TE §7): the
//! Grafana dashboards, their provisioning file and the Prometheus rule
//! files, embedded so that `merezha telemetry export` (and the scaffold,
//! SC-S4) writes the ones that match this host's catalog.
//!
//! The queries use the Prometheus 3 / Mimir OTLP translation of the
//! [`crate::names`] catalog; `tests/artifacts.rs` checks every metric and
//! label they name against it.
use std::{
    fs,
    io::{self, Write},
    path::Path,
};

/// One embedded file: its path below the export directory and its bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Artifact {
    /// Relative, `/`-separated: `dashboards.yaml`, `dashboards/*.json`,
    /// `rules/*.yaml`.
    pub path: &'static str,
    pub contents: &'static str,
}

macro_rules! artifacts {
    ($($path:literal),* $(,)?) => {
        /// Every exported file, in export order: `dashboards.yaml`, the
        /// dashboards, then the rule files.
        pub const ARTIFACTS: &[Artifact] = &[$(Artifact {
            path: $path,
            contents: include_str!(concat!("../../../deploy/telemetry/", $path)),
        }),*];
    };
}

artifacts![
    "dashboards.yaml",
    "dashboards/merezha-overview.json",
    "dashboards/merezha-application.json",
    "dashboards/merezha-data-jobs.json",
    "dashboards/merezha-delivery.json",
    "dashboards/merezha-telemetry.json",
    "rules/merezha-recording.yaml",
    "rules/merezha-alerts.yaml",
];

/// Why an export stopped: the file it was writing (`None`: the export
/// directory itself) and the I/O error kind. Never the OS message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportError {
    pub path: Option<&'static str>,
    pub kind: io::ErrorKind,
}

/// Write every [`ARTIFACTS`] file below `dir`, creating `dir`,
/// `dir/dashboards` and `dir/rules` as needed, and return the paths
/// written. Files of the same names are replaced; other files are left
/// alone, so `dir` may hold more (the scaffold's `lgtm.Dockerfile`).
///
/// Each file is written to a temporary sibling first and renamed over its
/// target, so a reader never sees half a file and an existing symbolic link
/// at the target is replaced rather than followed.
///
/// ```
/// let dir = std::env::temp_dir().join(format!("merezha-artifacts-doc-{}", std::process::id()));
/// let written = merezha_telemetry::artifacts::export(&dir).unwrap();
/// assert_eq!(written[0], "dashboards.yaml");
/// assert!(dir.join("rules/merezha-alerts.yaml").is_file());
/// std::fs::remove_dir_all(&dir).unwrap();
/// ```
pub fn export(dir: &Path) -> Result<Vec<&'static str>, ExportError> {
    fs::create_dir_all(dir).map_err(|e| ExportError {
        path: None,
        kind: e.kind(),
    })?;
    let mut written = Vec::with_capacity(ARTIFACTS.len());
    for artifact in ARTIFACTS {
        write_one(dir, artifact).map_err(|e| ExportError {
            path: Some(artifact.path),
            kind: e.kind(),
        })?;
        written.push(artifact.path);
    }
    Ok(written)
}

fn write_one(dir: &Path, artifact: &Artifact) -> io::Result<()> {
    let target = artifact
        .path
        .split('/')
        .fold(dir.to_path_buf(), |path, part| path.join(part));
    let parent = target.parent().unwrap_or(dir);
    fs::create_dir_all(parent)?;
    let name = target
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("artifact");
    let temporary = parent.join(format!(".{name}.export-{}", std::process::id()));
    let result = write_new(&temporary, artifact.contents.as_bytes())
        .and_then(|()| fs::rename(&temporary, &target));
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// Create `path` afresh (removing a leftover file or link of the same
/// name, never following it) and write `bytes` into it.
fn write_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if fs::symlink_metadata(path).is_ok() {
        fs::remove_file(path)?;
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_are_relative_and_unique() {
        let mut seen = std::collections::BTreeSet::new();
        for artifact in ARTIFACTS {
            assert!(seen.insert(artifact.path), "{} twice", artifact.path);
            assert!(!artifact.path.starts_with('/') && !artifact.path.contains(".."));
            assert!(
                artifact.path == "dashboards.yaml"
                    || (artifact.path.starts_with("dashboards/")
                        && artifact.path.ends_with(".json"))
                    || (artifact.path.starts_with("rules/") && artifact.path.ends_with(".yaml")),
                "{}",
                artifact.path
            );
            assert!(!artifact.contents.is_empty());
        }
    }
}
