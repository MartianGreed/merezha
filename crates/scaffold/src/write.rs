//! Writing the project whole or not at all (§2.4).
//!
//! Every file goes into `<parent>/.NAME.merezha-new-<16 hex>/project`. The
//! staging directory is created with mode 0700 and fails if it exists, so
//! `.env` is never readable by anyone else while it is written; the
//! `project` directory inside it is created with mode `0755 & !umask`, the
//! mode the project keeps. Files are created with `O_EXCL` at `0644` (`0755`
//! under `scripts/`) minus the umask; `.env` last, after the `.gitignore`
//! that lists it, at exactly 0600. Then `project` is renamed onto the
//! target: POSIX `rename` replaces an empty directory and fails on a
//! non-empty one, so a target filled after the checks fails the command
//! instead of mixing files. On any failure the staging directory is
//! removed.
use crate::{Error, render::Rendered};
use std::{
    fs::{self, DirBuilder, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

/// Mode of the staging directory and of `.env`.
pub const STAGING_MODE: u32 = 0o700;
pub const SECRETS_MODE: u32 = 0o600;

/// The staging directory's name for application `name`.
pub fn staging_name(name: &str) -> Result<String, Error> {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).map_err(|_| Error::failed("random_unavailable"))?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!(".{name}.merezha-new-{hex}"))
}

/// A file to write: rendered, or `.env` with `secret` set.
pub(crate) struct Entry {
    pub file: Rendered,
    pub secret: bool,
}

/// Writes `entries` in order into a fresh staging directory under `parent`
/// and renames the result onto `target`.
pub(crate) fn write(
    parent: &Path,
    staging: &str,
    target: &Path,
    entries: &[Entry],
) -> Result<(), Error> {
    stage(parent, staging, entries)?.commit(target)
}

/// A project written into its staging directory, removed on drop unless
/// committed.
pub(crate) struct Staged {
    staging: PathBuf,
    project: PathBuf,
    committed: bool,
}

impl Staged {
    /// Renames the project onto `target` (`rename_failed`).
    pub(crate) fn commit(mut self, target: &Path) -> Result<(), Error> {
        fs::rename(&self.project, target).map_err(|_| Error::failed("rename_failed"))?;
        self.committed = true;
        // Empty now; a leftover empty directory is harmless.
        let _ = fs::remove_dir(&self.staging);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn paths(&self) -> (&Path, &Path) {
        (&self.staging, &self.project)
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_dir_all(&self.staging);
        }
    }
}

/// Creates `parent/staging` (0700, failing if it exists) and writes every
/// entry into its `project` directory (`write_failed`, nothing left).
pub(crate) fn stage(parent: &Path, staging: &str, entries: &[Entry]) -> Result<Staged, Error> {
    let staging = parent.join(staging);
    DirBuilder::new()
        .mode(STAGING_MODE)
        .create(&staging)
        .map_err(|_| Error::failed("write_failed"))?;
    let mut staged = Staged {
        project: staging.join("project"),
        staging,
        committed: false,
    };
    staged.project = fill(&staged.staging, entries)?;
    Ok(staged)
}

/// Writes every entry into `staging/project`; returns that directory.
fn fill(staging: &Path, entries: &[Entry]) -> Result<PathBuf, Error> {
    let failed = || Error::failed("write_failed");
    // The umask cannot add bits, but it can remove the owner's: set 0700
    // exactly, then check.
    fs::set_permissions(staging, fs::Permissions::from_mode(STAGING_MODE)).map_err(|_| failed())?;
    let mode = fs::symlink_metadata(staging)
        .map_err(|_| failed())?
        .permissions()
        .mode();
    if mode & 0o777 != STAGING_MODE {
        return Err(failed());
    }
    let project = staging.join("project");
    DirBuilder::new()
        .mode(0o755)
        .create(&project)
        .map_err(|_| failed())?;
    for entry in entries {
        let path = project.join(&entry.file.path);
        if let Some(directory) = path.parent() {
            DirBuilder::new()
                .recursive(true)
                .mode(0o755)
                .create(directory)
                .map_err(|_| failed())?;
        }
        let mode = if entry.secret {
            SECRETS_MODE
        } else {
            entry.file.mode
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&path)
            .map_err(|_| failed())?;
        if entry.secret {
            file.set_permissions(fs::Permissions::from_mode(SECRETS_MODE))
                .map_err(|_| failed())?;
            let mode = file.metadata().map_err(|_| failed())?.permissions().mode();
            if mode & 0o777 != SECRETS_MODE {
                return Err(failed());
            }
        }
        file.write_all(&entry.file.contents).map_err(|_| failed())?;
        if entry.secret {
            file.sync_all().map_err(|_| failed())?;
        }
    }
    Ok(project)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn entry(path: &str, mode: u32, secret: bool) -> Entry {
        Entry {
            file: Rendered {
                path: path.to_owned(),
                contents: format!("{path}\n").into_bytes(),
                mode,
            },
            secret,
        }
    }

    fn mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
    }

    /// The bits the umask leaves: the mode of a file asked for 0777.
    fn allowed(dir: &Path) -> u32 {
        let probe = dir.join("umask-probe");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o777)
            .open(&probe)
            .unwrap();
        let bits = mode(&probe);
        fs::remove_file(probe).unwrap();
        bits
    }

    fn listing(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn stages_with_private_modes_then_renames() {
        let root = tempfile::tempdir().unwrap();
        let allowed = allowed(root.path());
        let entries = [
            entry(".gitignore", 0o644, false),
            entry("notifications/templates/a.txt", 0o644, false),
            entry("scripts/dev.sh", 0o755, false),
            entry(".env", SECRETS_MODE, true),
        ];
        let staged = stage(root.path(), ".acme.merezha-new-0", &entries).unwrap();
        let (staging, project) = staged.paths();
        assert_eq!(mode(staging), STAGING_MODE);
        assert_eq!(mode(project), 0o755 & allowed);
        assert_eq!(
            mode(&project.join("notifications/templates")),
            0o755 & allowed
        );
        assert_eq!(mode(&project.join(".gitignore")), 0o644 & allowed);
        assert_eq!(mode(&project.join("scripts/dev.sh")), 0o755 & allowed);
        assert_eq!(mode(&project.join(".env")), SECRETS_MODE);
        let target = root.path().join("acme");
        staged.commit(&target).unwrap();
        assert_eq!(listing(root.path()), ["acme"]);
        assert_eq!(fs::read_to_string(target.join(".env")).unwrap(), ".env\n");
        assert_eq!(mode(&target), 0o755 & allowed);
    }

    #[test]
    fn a_failure_mid_write_leaves_nothing_behind() {
        let root = tempfile::tempdir().unwrap();
        // The second `a` fails O_EXCL after three files were written.
        let entries = [
            entry("a", 0o644, false),
            entry("dir/b", 0o644, false),
            entry(".env", SECRETS_MODE, true),
            entry("a", 0o644, false),
        ];
        let target = root.path().join("acme");
        let error = write(root.path(), ".acme.merezha-new-1", &target, &entries).unwrap_err();
        assert_eq!(error.code, "write_failed");
        assert!(
            listing(root.path()).is_empty(),
            "{:?}",
            listing(root.path())
        );
        // A file where a directory must go fails the same way.
        let entries = [entry("a", 0o644, false), entry("a/b", 0o644, false)];
        let error = write(root.path(), ".acme.merezha-new-2", &target, &entries).unwrap_err();
        assert_eq!(error.code, "write_failed");
        assert!(listing(root.path()).is_empty());
    }

    #[test]
    fn an_existing_staging_directory_is_never_reused() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join(".acme.merezha-new-3")).unwrap();
        fs::write(root.path().join(".acme.merezha-new-3/keep"), "x").unwrap();
        let target = root.path().join("acme");
        let error = write(
            root.path(),
            ".acme.merezha-new-3",
            &target,
            &[entry("a", 0o644, false)],
        )
        .unwrap_err();
        assert_eq!(error.code, "write_failed");
        assert_eq!(listing(&root.path().join(".acme.merezha-new-3")), ["keep"]);
        assert!(!target.exists());
    }

    #[test]
    fn a_target_filled_before_the_rename_fails_and_keeps_its_files() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("acme");
        let staged = stage(
            root.path(),
            ".acme.merezha-new-4",
            &[entry("a", 0o644, false)],
        )
        .unwrap();
        fs::create_dir(&target).unwrap();
        fs::write(target.join("theirs"), "x").unwrap();
        assert_eq!(staged.commit(&target).unwrap_err().code, "rename_failed");
        assert_eq!(listing(root.path()), ["acme"]);
        assert_eq!(listing(&target), ["theirs"]);
        // An empty directory is replaced.
        fs::remove_file(target.join("theirs")).unwrap();
        let staged = stage(
            root.path(),
            ".acme.merezha-new-5",
            &[entry("a", 0o644, false)],
        )
        .unwrap();
        staged.commit(&target).unwrap();
        assert_eq!(listing(&target), ["a"]);
        assert_eq!(listing(root.path()), ["acme"]);
    }
}
