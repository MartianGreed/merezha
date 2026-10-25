//! Private directories for the local root (and the content block's spool
//! directory, which follows the same rules).
use crate::errors::{ROOT_INSECURE, ROOT_UNUSABLE};
use merezha_domain::Failure;
use std::path::{Path, PathBuf};

/// Create `path` with mode 0700 if missing (parents included) and return
/// its canonical form. Fails permanent `blob_root_unusable` when `path` is
/// relative, cannot be created or is not a directory, and
/// `blob_root_insecure` when it is a symbolic link (read with `lstat`, so a
/// link planted in a shared directory such as `/tmp` is never followed),
/// is owned by another user than the process's effective one, or grants
/// group or others any permission (mode `& 0o077`). The 0700 directory is
/// the access boundary; files and subdirectories below it follow the
/// process umask.
pub fn ensure_private_dir(path: &Path) -> Result<PathBuf, Failure> {
    if !path.is_absolute() {
        return Err(Failure::permanent(ROOT_UNUSABLE));
    }
    if std::fs::symlink_metadata(path).is_err() {
        create_private(path).map_err(|_| Failure::permanent(ROOT_UNUSABLE))?;
    }
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| Failure::permanent(ROOT_UNUSABLE))?;
    if metadata.file_type().is_symlink() {
        return Err(Failure::permanent(ROOT_INSECURE));
    }
    if !metadata.is_dir() {
        return Err(Failure::permanent(ROOT_UNUSABLE));
    }
    if insecure(&metadata) {
        return Err(Failure::permanent(ROOT_INSECURE));
    }
    std::fs::canonicalize(path).map_err(|_| Failure::permanent(ROOT_UNUSABLE))
}

/// Whether a directory is not this process's own: open to group or
/// others, or owned by another user.
pub(crate) fn insecure(metadata: &std::fs::Metadata) -> bool {
    open_to_others(metadata) || !owned_by(metadata, effective_uid())
}

/// The process's effective user id.
#[cfg(unix)]
fn effective_uid() -> u32 {
    // SAFETY: `geteuid` takes no arguments, cannot fail and touches no
    // memory of ours (POSIX.1-2008).
    unsafe { libc::geteuid() }
}
#[cfg(not(unix))]
fn effective_uid() -> u32 {
    0
}

/// Whether `metadata` belongs to user `uid`.
#[cfg(unix)]
fn owned_by(metadata: &std::fs::Metadata, uid: u32) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.uid() == uid
}
#[cfg(not(unix))]
fn owned_by(_: &std::fs::Metadata, _: u32) -> bool {
    true
}

#[cfg(unix)]
fn create_private(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
}
#[cfg(not(unix))]
fn create_private(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

/// Whether group or others have any permission on the directory.
#[cfg(unix)]
pub(crate) fn open_to_others(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o077 != 0
}
#[cfg(not(unix))]
pub(crate) fn open_to_others(_: &std::fs::Metadata) -> bool {
    false
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn a_missing_root_is_created_private() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("a/b/media");
        let canonical = ensure_private_dir(&root).unwrap();
        assert_eq!(mode(&root), 0o700);
        assert_eq!(mode(&dir.path().join("a")), 0o700);
        assert_eq!(canonical, std::fs::canonicalize(&root).unwrap());
    }

    #[test]
    fn roots_open_to_group_or_others_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        for (bits, ok) in [
            (0o700, true),
            (0o500, true),
            (0o755, false),
            (0o750, false),
            (0o710, false),
            (0o701, false),
            (0o770, false),
            (0o702, false),
        ] {
            let root = dir.path().join(format!("m{bits:o}"));
            std::fs::create_dir(&root).unwrap();
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(bits)).unwrap();
            let result = ensure_private_dir(&root);
            if ok {
                assert!(result.is_ok(), "{bits:o}");
            } else {
                assert_eq!(result.unwrap_err().code, "blob_root_insecure", "{bits:o}");
            }
        }
    }

    #[test]
    fn symlinks_and_directories_of_other_users_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(ensure_private_dir(&real).is_ok());
        // A link to a private directory is refused, never followed.
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert_eq!(
            ensure_private_dir(&link).unwrap_err().code,
            "blob_root_insecure"
        );
        // A dangling link is not created through either.
        let dangling = dir.path().join("dangling");
        std::os::unix::fs::symlink(dir.path().join("nowhere"), &dangling).unwrap();
        assert_eq!(
            ensure_private_dir(&dangling).unwrap_err().code,
            "blob_root_insecure"
        );
        assert!(!dir.path().join("nowhere").exists());
        // Ownership: ours passes, any other uid is refused.
        let metadata = std::fs::symlink_metadata(&real).unwrap();
        assert!(owned_by(&metadata, effective_uid()));
        assert!(!owned_by(&metadata, effective_uid().wrapping_add(1)));
        assert!(!insecure(&metadata));
    }

    #[test]
    fn relative_paths_and_files_are_unusable() {
        assert_eq!(
            ensure_private_dir(Path::new("media")).unwrap_err().code,
            "blob_root_unusable"
        );
        let file = tempfile::NamedTempFile::new().unwrap();
        assert_eq!(
            ensure_private_dir(file.path()).unwrap_err().code,
            "blob_root_unusable"
        );
    }
}
