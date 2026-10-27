//! Where the project goes, checked before anything is written (§2.3).
use crate::Error;
use std::{
    io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

/// A checked target: `dir` is absolute, below the canonical `parent`, and
/// either missing or an empty directory that is not the working directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub dir: PathBuf,
    pub parent: PathBuf,
    /// The permission bits of an existing empty directory, which the
    /// project keeps (narrowed, never widened); `None` for a new one.
    pub mode: Option<u32>,
}

/// Resolves `dir` (relative to `cwd`) and refuses a target that is a
/// symbolic link, a file, a non-empty directory or the working directory,
/// or whose parent directory does not exist. The parent is canonicalized,
/// so links above the target (`/tmp` on macOS) are fine.
pub fn resolve(dir: &Path, cwd: &Path) -> Result<Target, Error> {
    let path = if dir.is_absolute() {
        dir.to_owned()
    } else {
        cwd.join(dir)
    };
    if let Some(target) = existing(&path, cwd)? {
        return Ok(target);
    }
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Err(Error::invalid("parent_missing"));
    };
    let parent = match std::fs::canonicalize(parent) {
        Ok(parent) if parent.is_dir() => parent,
        _ => return Err(Error::invalid("parent_missing")),
    };
    let dir = parent.join(name);
    // The canonical parent may name another directory than the given path
    // did (a `..` after a link); check what is really there.
    if let Some(target) = existing(&dir, cwd)? {
        return Ok(target);
    }
    Ok(Target {
        dir,
        parent,
        mode: None,
    })
}

/// The checked target when something exists at `path`, `None` when nothing
/// does.
fn existing(path: &Path, cwd: &Path) -> Result<Option<Target>, Error> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        // A component of the path is a file, or not searchable.
        Err(_) => return Err(Error::invalid("parent_missing")),
    };
    let kind = metadata.file_type();
    if kind.is_symlink() {
        return Err(Error::invalid("target_is_symlink"));
    }
    if !kind.is_dir() {
        return Err(Error::invalid("target_not_directory"));
    }
    let dir = std::fs::canonicalize(path).map_err(|_| Error::invalid("parent_missing"))?;
    if std::fs::canonicalize(cwd).is_ok_and(|cwd| cwd == dir) {
        return Err(Error::invalid("target_is_working_directory"));
    }
    let empty = std::fs::read_dir(&dir)
        .map_err(|_| Error::invalid("target_not_empty"))?
        .next()
        .is_none();
    if !empty {
        return Err(Error::invalid("target_not_empty"));
    }
    let parent = dir
        .parent()
        .ok_or_else(|| Error::invalid("target_not_empty"))?
        .to_owned();
    Ok(Some(Target {
        dir,
        parent,
        mode: Some(metadata.permissions().mode() & 0o7777),
    }))
}
