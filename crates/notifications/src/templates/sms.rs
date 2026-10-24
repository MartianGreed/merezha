//! SMS encoding and segment counting: GSM 03.38 7-bit with its extension
//! table, else UCS-2. A single message holds 160 septets or 70 UTF-16 units;
//! concatenated parts hold 153 septets or 67 units, and an escape pair or a
//! surrogate pair never splits across parts.

/// The alphabet a message is sent in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Gsm7,
    Ucs2,
}
impl Encoding {
    pub fn as_str(self) -> &'static str {
        match self {
            Encoding::Gsm7 => "gsm7",
            Encoding::Ucs2 => "ucs2",
        }
    }
}

const BASIC: &str = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà";
const EXTENSION: &str = "\u{c}^{}\\[~]|€";

/// Septets of `c` in GSM-7, `None` outside the alphabet.
fn septets(c: char) -> Option<usize> {
    if BASIC.contains(c) {
        Some(1)
    } else if EXTENSION.contains(c) {
        Some(2)
    } else {
        None
    }
}

/// Pack `units` (each the size of one character) into parts of `single`
/// or, when longer, `multi` units.
fn pack(units: &[usize], single: usize, multi: usize) -> u32 {
    let total: usize = units.iter().sum();
    if total <= single {
        return 1;
    }
    let mut parts = 1u32;
    let mut used = 0usize;
    for size in units {
        if used + size > multi {
            parts += 1;
            used = 0;
        }
        used += size;
    }
    parts
}

/// The encoding and the number of parts `text` is sent as.
///
/// ```
/// use merezha_notifications::templates::{Encoding, segments};
/// assert_eq!(segments("Hello"), (Encoding::Gsm7, 1));
/// assert_eq!(segments(&"a".repeat(161)), (Encoding::Gsm7, 2));
/// assert_eq!(segments("Привет"), (Encoding::Ucs2, 1));
/// ```
pub fn segments(text: &str) -> (Encoding, u32) {
    if let Some(units) = text.chars().map(septets).collect::<Option<Vec<_>>>() {
        return (Encoding::Gsm7, pack(&units, 160, 153));
    }
    let units: Vec<usize> = text.chars().map(char::len_utf16).collect();
    (Encoding::Ucs2, pack(&units, 70, 67))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_follow_gsm7_and_ucs2_limits() {
        assert_eq!(segments(&"a".repeat(160)), (Encoding::Gsm7, 1));
        assert_eq!(segments(&"a".repeat(306)), (Encoding::Gsm7, 2));
        assert_eq!(segments(&"a".repeat(307)), (Encoding::Gsm7, 3));
        // An extension character counts twice and never splits.
        assert_eq!(segments(&"€".repeat(80)), (Encoding::Gsm7, 1));
        // 306 septets fit two parts, but the escape pair at septet 153
        // moves to the second part and pushes the last septet to a third.
        let a = "a".repeat(152);
        assert_eq!(segments(&format!("{a}€{a}")), (Encoding::Gsm7, 3));
        assert_eq!(segments(&"é".repeat(70)), (Encoding::Gsm7, 1));
        assert_eq!(segments(&"ç".repeat(70)), (Encoding::Ucs2, 1));
        assert_eq!(segments(&"ç".repeat(71)), (Encoding::Ucs2, 2));
        assert_eq!(segments(&"😀".repeat(35)), (Encoding::Ucs2, 1));
        assert_eq!(segments(&"😀".repeat(36)), (Encoding::Ucs2, 2));
    }
}
