//! The kernel's duration type (R12).
use std::time::Duration;

/// Parse `"500ms"`, `"30s"`, `"15m"`, `"2h"` or `"7d"`: a decimal integer
/// and one unit, nothing else. `None` for anything else or on overflow.
///
/// ```
/// use merezha_platform::config::parse_duration;
/// use std::time::Duration;
/// assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
/// assert_eq!(parse_duration("7d"), Some(Duration::from_secs(7 * 86_400)));
/// assert_eq!(parse_duration("1.5s"), None);
/// ```
pub fn parse_duration(text: &str) -> Option<Duration> {
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (digits, unit) = text.split_at(split);
    if digits.is_empty() || digits.len() > 20 {
        return None;
    }
    let value: u64 = digits.parse().ok()?;
    let millis_per_unit: u64 = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    value
        .checked_mul(millis_per_unit)
        .map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_take_one_integer_and_one_unit() {
        for (text, millis) in [
            ("500ms", 500),
            ("0s", 0),
            ("30s", 30_000),
            ("15m", 900_000),
            ("2h", 7_200_000),
            ("7d", 604_800_000),
            ("365d", 31_536_000_000),
        ] {
            assert_eq!(
                parse_duration(text),
                Some(Duration::from_millis(millis)),
                "{text}"
            );
        }
        for bad in [
            "",
            "5",
            "ms",
            "s5",
            "5 s",
            " 5s",
            "5s ",
            "1.5s",
            "-1s",
            "5S",
            "5sec",
            "5w",
            "99999999999999999999d",
            "1s1ms",
        ] {
            assert_eq!(parse_duration(bad), None, "{bad}");
        }
    }
}
