//! Which merezha revision a generated project pins (scaffold §2.9).
//!
//! This file has no dependencies beyond `std` and no reference to the rest
//! of the crate: the source pin tests copy it, with `crates/cli/build.rs`,
//! into a probe crate built from a temporary git repository.

/// Whether `text` can be pinned: a full SHA-1 commit id, 40 lowercase hex
/// digits. `crates/cli/build.rs` also records 64-digit SHA-256 ids, but the
/// project's `merezha-domain` dependency fetches from GitHub, which serves
/// SHA-1 repositories only, so a SHA-256 revision could never resolve.
pub fn is_pinnable_rev(text: &str) -> bool {
    text.len() == 40 && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The revision to pin: `--merezha-rev` when given (`invalid_rev` unless it
/// is [pinnable](is_pinnable_rev)), else the revision the binary was built
/// from. A binary that does not know its revision (a dirty or unknown tree)
/// or knows one that cannot be pinned gives `source_rev_unknown`, so nothing
/// is pinned to a revision that is not the source.
pub fn select_rev(given: Option<&str>, built_from: Option<&str>) -> Result<String, &'static str> {
    match (given, built_from) {
        (Some(rev), _) if is_pinnable_rev(rev) => Ok(rev.to_owned()),
        (Some(_), _) => Err("invalid_rev"),
        (None, Some(rev)) if is_pinnable_rev(rev) => Ok(rev.to_owned()),
        (None, _) => Err("source_rev_unknown"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA1: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn only_full_sha1_ids_are_pinned() {
        let sha256 = "0123456789abcdef".repeat(4);
        assert_eq!(select_rev(Some(SHA1), None), Ok(SHA1.to_owned()));
        assert_eq!(select_rev(None, Some(SHA1)), Ok(SHA1.to_owned()));
        assert_eq!(select_rev(Some(SHA1), Some(&sha256)), Ok(SHA1.to_owned()));
        for bad in [&sha256[..], &SHA1[..39], &SHA1.to_uppercase(), "main", ""] {
            assert_eq!(
                select_rev(Some(bad), Some(SHA1)),
                Err("invalid_rev"),
                "{bad}"
            );
        }
        assert_eq!(select_rev(None, Some(&sha256)), Err("source_rev_unknown"));
        assert_eq!(select_rev(None, None), Err("source_rev_unknown"));
    }
}
