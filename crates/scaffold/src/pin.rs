//! Which merezha revision a generated project pins (scaffold §2.9).
//!
//! This file has no dependencies beyond `std` and no reference to the rest
//! of the crate: the source pin tests copy it, with `crates/cli/build.rs`,
//! into a probe crate built from a temporary git repository.

/// Whether `text` is a full lowercase commit id: 40 hex digits (SHA-1), or
/// 64 (a SHA-256 repository), as `crates/cli/build.rs` records them.
pub fn is_commit_id(text: &str) -> bool {
    matches!(text.len(), 40 | 64) && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The revision to pin: `--merezha-rev` when given (`invalid_rev` unless it
/// is a full commit id), else the revision the binary was built from.
/// A binary that does not know its revision (a dirty or unknown tree)
/// gives `source_rev_unknown`, so nothing is pinned to a revision that is
/// not the source.
pub fn select_rev(given: Option<&str>, built_from: Option<&str>) -> Result<String, &'static str> {
    match (given, built_from) {
        (Some(rev), _) if is_commit_id(rev) => Ok(rev.to_owned()),
        (Some(_), _) => Err("invalid_rev"),
        (None, Some(rev)) if is_commit_id(rev) => Ok(rev.to_owned()),
        (None, _) => Err("source_rev_unknown"),
    }
}
