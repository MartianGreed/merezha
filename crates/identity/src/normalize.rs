//! Normalization rules (section 2): email addresses, display names,
//! locales and `return_to` paths. Pure functions; every refusal is a stable
//! detail code (`invalid`, `too_long`), never an echo of the input.
use unicode_normalization::UnicodeNormalization;

/// Longest normalized address, local part and display form.
pub const MAX_EMAIL_BYTES: usize = 254;
pub const MAX_LOCAL_BYTES: usize = 64;
const MAX_DISPLAY_EMAIL_BYTES: usize = 320;
/// Longest `return_to`.
pub const MAX_RETURN_TO_BYTES: usize = 512;
/// Display names: 1 to 128 scalar values.
pub const MAX_DISPLAY_NAME_CHARS: usize = 128;

/// A normalized email address. `Debug` never shows it: addresses are
/// personal data.
#[derive(Clone, PartialEq, Eq)]
pub struct Email {
    /// Trimmed NFC input, kept for display.
    display: String,
    /// The uniqueness key: lowercase local part, `@`, ASCII lowercase domain.
    normalized: String,
    /// Where mail goes: the display local part with the ASCII domain.
    deliverable: String,
}
impl std::fmt::Debug for Email {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Email(<redacted>)")
    }
}
impl Email {
    pub fn display(&self) -> &str {
        &self.display
    }
    pub fn normalized(&self) -> &str {
        &self.normalized
    }
    pub fn deliverable(&self) -> &str {
        &self.deliverable
    }
    /// The local part of the normalized address.
    pub fn local(&self) -> &str {
        self.normalized
            .rsplit_once('@')
            .map_or("", |(local, _)| local)
    }
    /// The labels of the domain without its last one (the TLD).
    pub fn domain_labels(&self) -> Vec<&str> {
        let domain = self.normalized.rsplit_once('@').map_or("", |(_, d)| d);
        let mut labels: Vec<&str> = domain.split('.').collect();
        labels.pop();
        labels
    }
}

/// Normalize an address: trim, NFC, split at the last `@`, IDNA the domain
/// to lowercase ASCII, lowercase the local part. No provider folding. A
/// second `@`, whitespace, control characters and angle brackets are
/// refused, so every accepted address is deliverable as is. Fails
/// with `invalid` or `too_long`.
pub fn email(input: &str) -> Result<Email, &'static str> {
    let display: String = input.trim().nfc().collect();
    if display.len() > MAX_DISPLAY_EMAIL_BYTES {
        return Err("too_long");
    }
    let (local, domain) = display.rsplit_once('@').ok_or("invalid")?;
    if local.is_empty() || domain.is_empty() {
        return Err("invalid");
    }
    if local
        .chars()
        .chain(domain.chars())
        .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '<' | '>' | '@'))
    {
        return Err("invalid");
    }
    let ascii = idna::domain_to_ascii(domain).map_err(|_| "invalid")?;
    if ascii.is_empty()
        || !ascii.contains('.')
        || ascii.split('.').any(str::is_empty)
        || !ascii
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
    {
        return Err("invalid");
    }
    let lower = local.to_lowercase();
    if lower.len() > MAX_LOCAL_BYTES {
        return Err("too_long");
    }
    let normalized = format!("{lower}@{ascii}");
    if normalized.len() > MAX_EMAIL_BYTES {
        return Err("too_long");
    }
    let deliverable = format!("{local}@{ascii}");
    Ok(Email {
        display,
        normalized,
        deliverable,
    })
}

/// NFC, 1 to 128 scalar values, no control characters. Fails `invalid` or
/// `too_long`.
pub fn display_name(input: &str) -> Result<String, &'static str> {
    let name: String = input.nfc().collect();
    let count = name.chars().count();
    if count == 0 || name.trim().is_empty() {
        return Err("invalid");
    }
    if count > MAX_DISPLAY_NAME_CHARS {
        return Err("too_long");
    }
    if name.chars().any(char::is_control) {
        return Err("invalid");
    }
    Ok(name)
}

/// A BCP 47 tag shape such as `fr` or `pt-BR`: 2 to 35 bytes of ASCII
/// letters, digits and `-`, starting with a letter.
pub fn locale(input: &str) -> Result<String, &'static str> {
    merezha_platform::ports::Locale::new(input)
        .map(|l| l.as_str().to_owned())
        .map_err(|_| "invalid")
}

/// A `return_to` value: a relative path that starts with `/` and not with
/// `//` or `/\`, at most 512 bytes, with no backslash, whitespace or control
/// character, and the same rules again after one round of percent-decoding
/// so encoded variants (`/%2F%2Fevil`) cannot become a scheme-relative URL.
pub fn return_to(input: &str) -> Result<String, &'static str> {
    if input.len() > MAX_RETURN_TO_BYTES {
        return Err("too_long");
    }
    fn safe(path: &str) -> bool {
        path.starts_with('/')
            && !path.starts_with("//")
            && !path.contains('\\')
            && !path.chars().any(|c| c.is_whitespace() || c.is_control())
    }
    if !safe(input) {
        return Err("invalid");
    }
    let decoded = percent_decode(input).ok_or("invalid")?;
    if !safe(&decoded) {
        return Err("invalid");
    }
    Ok(input.to_owned())
}

/// One round of percent-decoding into UTF-8; `None` for a malformed escape
/// or invalid UTF-8.
fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = input.get(index + 1..index + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn email_normalization_table() {
        let cases = [
            ("  Ada@Example.COM ", "ada@example.com", "Ada@example.com"),
            (
                "ada+tag@example.com",
                "ada+tag@example.com",
                "ada+tag@example.com",
            ),
            ("a.d.a@gmail.com", "a.d.a@gmail.com", "a.d.a@gmail.com"),
            (
                "ÄDA@bücher.example",
                "äda@xn--bcher-kva.example",
                "ÄDA@xn--bcher-kva.example",
            ),
            // NFC: a combining diaeresis composes.
            (
                "A\u{308}da@example.com",
                "äda@example.com",
                "Äda@example.com",
            ),
        ];
        for (input, normalized, deliverable) in cases {
            let email = email(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(email.normalized(), normalized, "{input}");
            assert_eq!(email.deliverable(), deliverable, "{input}");
        }
        assert_eq!(
            email("  Ada@Example.COM ").unwrap().display(),
            "Ada@Example.COM"
        );
        let long_local = format!("{}@example.com", "a".repeat(65));
        let long_total = format!("a@{}.com", "b".repeat(250));
        for (input, code) in [
            ("", "invalid"),
            ("ada", "invalid"),
            ("@example.com", "invalid"),
            ("ada@", "invalid"),
            ("ada@localhost", "invalid"),
            ("ada@exa mple.com", "invalid"),
            ("a da@example.com", "invalid"),
            ("ada@example..com", "invalid"),
            ("ada@exam\nple.com", "invalid"),
            ("<ada@example.com>", "invalid"),
            ("weird@name@example.org", "invalid"),
            (long_local.as_str(), "too_long"),
        ] {
            assert_eq!(email(input).unwrap_err(), code, "{input:?}");
        }
        assert!(email(&long_total).is_err());
        let e = email("ada@mail.example.co.uk").unwrap();
        assert_eq!(e.local(), "ada");
        assert_eq!(e.domain_labels(), vec!["mail", "example", "co"]);
        assert_eq!(format!("{e:?}"), "Email(<redacted>)");
    }

    #[test]
    fn display_names_and_locales() {
        assert_eq!(display_name("Ada Lovelace").unwrap(), "Ada Lovelace");
        assert_eq!(display_name("A\u{308}da").unwrap(), "Äda");
        assert_eq!(display_name("").unwrap_err(), "invalid");
        assert_eq!(display_name("   ").unwrap_err(), "invalid");
        assert_eq!(display_name("a\u{7}b").unwrap_err(), "invalid");
        assert_eq!(display_name(&"é".repeat(128)).unwrap().chars().count(), 128);
        assert_eq!(display_name(&"é".repeat(129)).unwrap_err(), "too_long");
        assert_eq!(locale("pt-BR").unwrap(), "pt-BR");
        assert_eq!(locale("1x").unwrap_err(), "invalid");
    }

    #[test]
    fn return_to_accepts_only_relative_paths() {
        for good in ["/", "/dashboard", "/a/b?c=d#e", "/caf%C3%A9", "/x%2Fy"] {
            assert_eq!(return_to(good).unwrap(), good);
        }
        for bad in [
            "",
            "dashboard",
            "//evil.example",
            "/\\evil.example",
            "\\\\evil.example",
            "https://evil.example",
            "http:/evil.example",
            "javascript:alert(1)",
            "/%2F%2Fevil.example",
            "/%2f/evil.example",
            "%2F%2Fevil.example",
            "/%5Cevil.example",
            "/\t/evil.example",
            "/%09/evil.example",
            "/ /evil",
            "/%zz",
            "/%C3",
        ] {
            assert!(return_to(bad).is_err(), "{bad:?}");
        }
        let long = format!("/{}", "a".repeat(512));
        assert_eq!(return_to(&long).unwrap_err(), "too_long");
    }
}
