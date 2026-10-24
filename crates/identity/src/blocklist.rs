//! The built-in breached-password list (section 7).
//!
//! Source: SecLists `Passwords/Common-Credentials/100k-most-used-passwords-NCSC.txt`
//! (commit 1a7bb9127eca9e6ff2fc0301c597fe6e16a0cb56, MIT, license in
//! `data/SecLists-LICENSE`), the 100,000 most common passwords of the UK
//! NCSC's breach corpus. The embedded file keeps the 46,483 distinct
//! entries of at least 8 characters after NFKC and lowercasing (shorter ones
//! can never pass the length floor), sorted, one per line, zlib-compressed
//! (148 KiB). It is decompressed once, on first use.
use std::{collections::HashSet, sync::OnceLock};

const COMPRESSED: &[u8] = include_bytes!("../data/passwords.zlib");
/// Bound on the decompressed list, against a corrupted build input.
const MAX_LIST_BYTES: usize = 1 << 20;

fn list() -> &'static HashSet<&'static str> {
    static LIST: OnceLock<HashSet<&'static str>> = OnceLock::new();
    static TEXT: OnceLock<String> = OnceLock::new();
    LIST.get_or_init(|| {
        let text = TEXT.get_or_init(|| {
            miniz_oxide::inflate::decompress_to_vec_zlib_with_limit(COMPRESSED, MAX_LIST_BYTES)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .unwrap_or_default()
        });
        text.split('\n').filter(|line| !line.is_empty()).collect()
    })
}

/// Whether `folded` (NFKC, lowercase) is on the built-in list.
pub fn contains(folded: &str) -> bool {
    list().contains(folded)
}

/// Entries on the built-in list.
pub fn len() -> usize {
    list().len()
}

/// Force decompression, so the first sign-up does not pay for it.
pub fn warm() {
    let _ = list();
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_embedded_list_decompresses_completely() {
        assert_eq!(super::len(), 46_483);
        assert!(super::contains("12345678"));
        assert!(super::contains("iloveyou"));
        assert!(!super::contains("zqvdkrnmwplh"));
        assert!(!super::contains(""));
    }
}
