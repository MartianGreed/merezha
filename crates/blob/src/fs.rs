//! Private directories for the local root (and the content block's spool
//! directory, which follows the same rules).
use crate::errors::{ROOT_INSECURE, ROOT_UNUSABLE};
use merezha_domain::Failure;
use std::path::{Path, PathBuf};

/// Create `path` with mode 0700 if missing (parents included) and return
/// its canonical form. Fails permanent `blob_root_unusable` when `path` is
/// relative, cannot be created or is not a directory, and
/// `blob_root_insecure` when the directory is writable by group or others.
pub fn ensure_private_dir(path: &Path) -> Result<PathBuf, Failure> {
    if !path.is_absolute() {
        return Err(Failure::permanent(ROOT_UNUSABLE));
    }
    if !path.exists() {
        create_private(path).map_err(|_| Failure::permanent(ROOT_UNUSABLE))?;
    }
    let metadata = std::fs::metadata(path).map_err(|_| Failure::permanent(ROOT_UNUSABLE))?;
    if !metadata.is_dir() {
        return Err(Failure::permanent(ROOT_UNUSABLE));
    }
    if writable_by_others(&metadata) {
        return Err(Failure::permanent(ROOT_INSECURE));
    }
    std::fs::canonicalize(path).map_err(|_| Failure::permanent(ROOT_UNUSABLE))
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

#[cfg(unix)]
fn writable_by_others(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o022 != 0
}
#[cfg(not(unix))]
fn writable_by_others(_: &std::fs::Metadata) -> bool {
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
    fn group_or_world_writable_roots_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        for (bits, ok) in [(0o700, true), (0o755, true), (0o770, false), (0o702, false)] {
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
