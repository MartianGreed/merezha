//! What an upload is (section 6, upload flow steps 2, 4 and 5): the
//! declared type, the stored filename, the sniffed type the bytes must
//! match, UTF-8 and `<svg` root checks for text types, and raster
//! dimensions read from the header without decoding pixels.
use std::path::Path;

/// Types accepted when no binary signature is recognized, provided the
/// bytes are valid UTF-8 (and an SVG has an `<svg` root).
pub const TEXT_TYPES: [&str; 4] = [
    "text/plain",
    "text/csv",
    "application/json",
    "image/svg+xml",
];
/// How much of the upload the signature check reads.
pub const SNIFF_BYTES: usize = 8192;
/// Most pixels on either side of a raster image; more is malformed.
pub const MAX_IMAGE_SIDE: usize = 50_000;
/// Longest stored filename, in bytes.
pub const MAX_FILENAME_BYTES: usize = 200;
/// Raster types whose dimensions are read.
const RASTER_TYPES: [&str; 5] = [
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/avif",
];

/// A bare lowercase MIME type, `type/subtype` of token characters.
pub(crate) fn valid_media_type(value: &str) -> bool {
    let token = |s: &str| {
        !s.is_empty()
            && s.len() <= 127
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$&^_.+-".contains(&b))
    };
    value
        .split_once('/')
        .is_some_and(|(kind, sub)| token(kind) && token(sub))
}

/// Types that a browser renders as active content on the API origin:
/// never `inline` (section 6).
pub(crate) fn active_type(media_type: &str) -> bool {
    media_type == "text/html"
        || media_type == "application/pdf"
        || media_type.ends_with("/xml")
        || media_type.ends_with("+xml")
}

/// The declared `Content-Type` without parameters, lowercased, with the
/// `image/jpg` alias read as `image/jpeg`; `None` when it is not a MIME
/// type.
pub(crate) fn declared_type(header: Option<&str>) -> Option<String> {
    let base = header?.split(';').next()?.trim().to_ascii_lowercase();
    let base = if base == "image/jpg" {
        "image/jpeg".to_owned()
    } else {
        base
    };
    valid_media_type(&base).then_some(base)
}

/// The type a binary signature in `head` names, `None` when no binary
/// signature matches. `infer`'s text guesses (HTML, XML, shell) are not
/// signatures: text is checked as UTF-8 instead, and an HTML file declared
/// `text/plain` is stored as plain text and served as an attachment.
pub(crate) fn signature(head: &[u8]) -> Option<&'static str> {
    let kind = infer::get(head)?;
    if kind.matcher_type() == infer::MatcherType::Text {
        return None;
    }
    Some(match kind.mime_type() {
        "image/jpg" => "image/jpeg",
        other => other,
    })
}

/// Whether bytes starting with `head`, all valid UTF-8 or not (`utf8`),
/// may be stored as `declared` (step 4).
pub(crate) fn matches(declared: &str, head: &[u8], utf8: bool) -> bool {
    match signature(head) {
        Some(found) => found == declared,
        None => {
            TEXT_TYPES.contains(&declared)
                && utf8
                && (declared != "image/svg+xml" || svg_root(head))
        }
    }
}

/// Whether the first element of an XML document starting with `head` is
/// `<svg`, past a byte order mark, an XML declaration, processing
/// instructions, comments and a doctype.
pub(crate) fn svg_root(head: &[u8]) -> bool {
    let mut rest = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    loop {
        let start = rest
            .iter()
            .position(|b| !b.is_ascii_whitespace())
            .unwrap_or(rest.len());
        rest = &rest[start..];
        let skip = |rest: &[u8], close: &[u8]| -> Option<usize> {
            rest.windows(close.len())
                .position(|w| w == close)
                .map(|at| at + close.len())
        };
        let advanced = if rest.starts_with(b"<?") {
            skip(rest, b"?>")
        } else if rest.starts_with(b"<!--") {
            skip(rest, b"-->")
        } else if rest.len() >= 9 && rest[..9].eq_ignore_ascii_case(b"<!doctype") {
            let close = rest.iter().position(|b| *b == b'>');
            let subset = rest.iter().position(|b| *b == b'[');
            match (subset, close) {
                (Some(open), Some(end)) if open < end => skip(rest, b"]>"),
                (_, Some(end)) => Some(end + 1),
                _ => None,
            }
        } else {
            return rest.starts_with(b"<svg")
                && rest
                    .get(4)
                    .is_some_and(|b| b.is_ascii_whitespace() || *b == b'>' || *b == b'/');
        };
        match advanced {
            Some(at) => rest = &rest[at..],
            None => return false,
        }
    }
}

/// Incremental UTF-8 validation across chunk boundaries.
#[derive(Debug, Default)]
pub(crate) struct Utf8Check {
    carry: Vec<u8>,
    invalid: bool,
}
impl Utf8Check {
    pub(crate) fn feed(&mut self, mut chunk: &[u8]) {
        if self.invalid {
            return;
        }
        // Complete a character split by the previous chunk.
        while !self.carry.is_empty() {
            let Some((first, rest)) = chunk.split_first() else {
                return;
            };
            self.carry.push(*first);
            chunk = rest;
            match std::str::from_utf8(&self.carry) {
                Ok(_) => self.carry.clear(),
                Err(e) if e.error_len().is_some() || self.carry.len() >= 4 => {
                    self.invalid = true;
                    return;
                }
                Err(_) => {}
            }
        }
        if let Err(e) = std::str::from_utf8(chunk) {
            if e.error_len().is_some() {
                self.invalid = true;
            } else {
                self.carry.extend_from_slice(&chunk[e.valid_up_to()..]);
            }
        }
    }
    /// Every byte fed was valid UTF-8, the last character complete.
    pub(crate) fn valid(&self) -> bool {
        !self.invalid && self.carry.is_empty()
    }
}

/// The extensions a stored filename may carry for `media_type`, the
/// canonical one first; empty for a type without a known one.
pub(crate) fn extensions(media_type: &str) -> &'static [&'static str] {
    match media_type {
        "image/png" => &["png"],
        "image/jpeg" => &["jpg", "jpeg", "jpe"],
        "image/gif" => &["gif"],
        "image/webp" => &["webp"],
        "image/avif" => &["avif"],
        "image/svg+xml" => &["svg"],
        "application/pdf" => &["pdf"],
        "video/mp4" => &["mp4", "m4v"],
        "video/webm" => &["webm"],
        "audio/mpeg" => &["mp3"],
        "audio/ogg" => &["ogg", "oga", "opus"],
        "text/plain" => &["txt", "text"],
        "text/csv" => &["csv"],
        "application/json" => &["json"],
        _ => &[],
    }
}

/// The stored filename for a requested `raw` name and the upload's type
/// (step 2): characters outside `[A-Za-z0-9._-]` become `-`, leading
/// punctuation goes, an extension that does not match the type is replaced
/// by the type's own, and the result is at most 200 bytes. A name with
/// nothing left is `file`.
pub(crate) fn stored_filename(raw: &str, media_type: &str) -> String {
    let mut reduced = String::with_capacity(raw.len().min(4 * MAX_FILENAME_BYTES));
    for c in raw.chars().take(4 * MAX_FILENAME_BYTES) {
        let keep = c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-');
        let c = if keep { c } else { '-' };
        if !(c == '-' && reduced.ends_with('-')) {
            reduced.push(c);
        }
    }
    let reduced = reduced.trim_start_matches(['.', '_', '-']);
    let known = extensions(media_type);
    let (stem, extension) = match reduced.rsplit_once('.') {
        Some((stem, ext)) if known.contains(&ext.to_ascii_lowercase().as_str()) => {
            (stem, Some(ext.to_ascii_lowercase()))
        }
        Some((stem, _)) if !known.is_empty() => (stem, None),
        _ => (reduced, None),
    };
    let extension = extension.or_else(|| known.first().map(|e| (*e).to_owned()));
    let room = MAX_FILENAME_BYTES - extension.as_ref().map_or(0, |e| e.len() + 1);
    let mut stem = stem.trim_end_matches('.').to_owned();
    stem.truncate(room);
    if stem.is_empty() {
        stem.push_str("file");
    }
    match extension {
        Some(extension) => format!("{stem}.{extension}"),
        None => stem,
    }
}

/// The raster dimensions of the spooled file at `path` for `media_type`:
/// `Ok(None)` for a type without them, `Err` for a header that does not
/// parse or a side of zero or over [`MAX_IMAGE_SIDE`]. Reads the header
/// only, never the pixels.
pub(crate) async fn dimensions(path: &Path, media_type: &str) -> Result<Option<(i32, i32)>, ()> {
    if !RASTER_TYPES.contains(&media_type) {
        return Ok(None);
    }
    let path = path.to_owned();
    let size = merezha_telemetry::spawn_blocking_in_span(move || imagesize::size(path))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;
    let side = |n: usize| -> Result<i32, ()> {
        if (1..=MAX_IMAGE_SIDE).contains(&n) {
            i32::try_from(n).map_err(|_| ())
        } else {
            Err(())
        }
    };
    Ok(Some((side(size.width)?, side(size.height)?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PNG: &[u8] = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR\0\0\0\x02\0\0\0\x03\x08\x02\0\0\0";

    #[test]
    fn declared_types_lose_parameters_and_the_jpg_alias() {
        assert_eq!(
            declared_type(Some("Image/JPG; charset=x")).as_deref(),
            Some("image/jpeg")
        );
        assert_eq!(
            declared_type(Some("text/plain;charset=utf-8")).as_deref(),
            Some("text/plain")
        );
        assert_eq!(declared_type(Some("text")), None);
        assert_eq!(declared_type(Some("text/*")), None);
        assert_eq!(declared_type(None), None);
    }

    #[test]
    fn signatures_must_match_and_text_guesses_are_not_signatures() {
        assert_eq!(signature(PNG), Some("image/png"));
        assert!(matches("image/png", PNG, false));
        assert!(!matches("image/jpeg", PNG, false));
        // HTML is text for the check: stored as the declared plain text.
        let html = b"<!DOCTYPE html><html><script>alert(1)</script></html>";
        assert_eq!(signature(html), None);
        assert!(matches("text/plain", html, true));
        assert!(!matches("text/plain", html, false));
        // A type without a signature that is not text is refused.
        assert!(!matches("video/mp4", b"hello", true));
        assert!(!matches("text/plain", PNG, true));
    }

    #[test]
    fn svg_needs_an_svg_root() {
        for good in [
            &b"<svg xmlns=\"http://www.w3.org/2000/svg\"/>"[..],
            b"\xef\xbb\xbf<?xml version=\"1.0\"?>\n<!-- c -->\n<!DOCTYPE svg [<!ENTITY a \"b\">]>\n<svg>",
            b"  <svg\n>",
        ] {
            assert!(svg_root(good), "{}", String::from_utf8_lossy(good));
            assert!(matches("image/svg+xml", good, true));
        }
        for bad in [
            &b"<html><svg>"[..],
            b"<svgx>",
            b"<?xml version=\"1.0\"",
            b"<!-- open",
            b"",
        ] {
            assert!(!svg_root(bad), "{}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn utf8_is_checked_across_chunks() {
        let text = "héllo wörld €".as_bytes();
        for split in 0..text.len() {
            let mut check = Utf8Check::default();
            check.feed(&text[..split]);
            check.feed(&text[split..]);
            assert!(check.valid(), "{split}");
        }
        let mut check = Utf8Check::default();
        check.feed(b"ok \xe2\x82");
        assert!(!check.valid(), "an unfinished character");
        let mut check = Utf8Check::default();
        check.feed(b"\xff");
        check.feed(b"ok");
        assert!(!check.valid());
    }

    #[test]
    fn filenames_are_reduced_with_an_extension_of_their_type() {
        for (raw, media_type, stored) in [
            ("team.jpg", "image/jpeg", "team.jpg"),
            ("Team Photo.JPEG", "image/jpeg", "Team-Photo.jpeg"),
            ("photo.png", "image/jpeg", "photo.jpg"),
            ("notes", "text/plain", "notes.txt"),
            ("../../etc/passwd", "text/plain", "etc-passwd.txt"),
            ("..", "text/plain", "file.txt"),
            ("été.csv", "text/csv", "t-.csv"),
            ("page.html", "text/plain", "page.txt"),
            ("x.svg", "image/svg+xml", "x.svg"),
        ] {
            assert_eq!(stored_filename(raw, media_type), stored, "{raw}");
        }
        let long = "a".repeat(500) + ".png";
        let stored = stored_filename(&long, "image/png");
        assert_eq!(stored.len(), 200);
        assert!(stored.ends_with(".png"));
        let pattern = regex::Regex::new("^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$").unwrap();
        for raw in ["", "-", "日本語", "a\u{0}b", "   .x", &"é".repeat(300)] {
            let stored = stored_filename(raw, "image/png");
            assert!(pattern.is_match(&stored), "{raw:?} -> {stored}");
        }
    }

    #[test]
    fn active_types_are_never_inline() {
        for t in [
            "text/html",
            "image/svg+xml",
            "application/xml",
            "text/xml",
            "application/xhtml+xml",
            "application/pdf",
        ] {
            assert!(active_type(t), "{t}");
        }
        for t in ["image/png", "text/plain", "video/mp4"] {
            assert!(!active_type(t), "{t}");
        }
    }
}
