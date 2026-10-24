//! Slug generation from a source field.

/// The slug the host generates for `source`, at most `max_length` bytes:
/// transliterated to ASCII with `deunicode`, lowercased, every run of
/// characters other than `[a-z0-9]` replaced by one `-`, and truncated at a
/// word boundary (a first word longer than `max_length` is cut). `None`
/// when nothing usable remains or `max_length` is 0.
///
/// ```
/// assert_eq!(
///     merezha_content::slugify("Notes de version 1.4 — été", 96).as_deref(),
///     Some("notes-de-version-1-4-ete")
/// );
/// assert_eq!(merezha_content::slugify("Hello wonderful world", 15).as_deref(), Some("hello-wonderful"));
/// assert_eq!(merezha_content::slugify("¿?!", 96), None);
/// ```
pub fn slugify(source: &str, max_length: usize) -> Option<String> {
    let ascii = deunicode::deunicode(source).to_ascii_lowercase();
    let words: Vec<&str> = ascii
        .split(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit()))
        .filter(|w| !w.is_empty())
        .collect();
    let mut slug = String::new();
    for word in words {
        let extra = if slug.is_empty() { 0 } else { 1 };
        if slug.len() + extra + word.len() > max_length {
            if slug.is_empty() {
                slug.push_str(&word[..max_length]);
            }
            break;
        }
        if extra == 1 {
            slug.push('-');
        }
        slug.push_str(word);
    }
    (!slug.is_empty()).then_some(slug)
}

#[cfg(test)]
mod tests {
    use super::slugify;
    use crate::scalar::is_slug;

    #[test]
    fn generated_slugs_are_valid_and_bounded() {
        for (source, max, expected) in [
            ("Release notes", 96, Some("release-notes")),
            ("  Déjà vu -- 2026!  ", 96, Some("deja-vu-2026")),
            ("Ünïcödé Straße", 96, Some("unicode-strasse")),
            ("supercalifragilistic", 5, Some("super")),
            ("ab cd ef", 5, Some("ab-cd")),
            ("ab cd ef", 4, Some("ab")),
            ("", 96, None),
            ("---", 96, None),
            ("x", 0, None),
        ] {
            let slug = slugify(source, max);
            assert_eq!(slug.as_deref(), expected, "{source:?}");
            if let Some(slug) = slug {
                assert!(is_slug(&slug) && slug.len() <= max, "{slug}");
            }
        }
    }
}
