//! Cron expressions and time zones for `[[jobs.schedules]]` and
//! `JobsHandle::schedule` (AP §B.7).
//!
//! Expressions have exactly five fields (minute, hour, day of month, month,
//! day of week), so the finest granularity is one minute. They are parsed
//! by `croner` with seconds disallowed, which also accepts `L`, `W`, `#`,
//! ranges, steps and month and weekday names. Zones are IANA names
//! resolved from the bundled tzdb (`jiff`'s `tzdb-bundle-always`), so they
//! resolve the same way on every platform.
//!
//! Daylight saving time follows croner's documented (OCPS, Vixie-style)
//! rules: in a spring-forward gap a fixed-time schedule runs at the first
//! valid minute after the gap and an interval schedule skips the missing
//! occurrences; in a fall-back overlap a fixed-time schedule runs once, at
//! its first wall-clock occurrence, and an interval schedule runs in both
//! passes. Searches move in real time, so pass the instant the previous
//! search returned.
use croner::{Cron, parser::CronParser, parser::Seconds};
use jiff::{Timestamp, tz::TimeZone};
use merezha_domain::Failure;

/// Longest accepted expression, in bytes.
pub const MAX_CRON_LEN: usize = 128;
/// Longest accepted zone name, in bytes.
pub const MAX_TIMEZONE_LEN: usize = 64;

/// Why a definition was refused, per field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CronProblem {
    /// Not five fields of the cron alphabet, refused by croner, or never
    /// fires (such as `0 0 30 2 *`). Code `invalid_cron`.
    Cron,
    /// Not shaped like a zone name. Code `invalid_timezone`.
    TimezoneShape,
    /// Shaped like a zone name but not in the tzdb. Code
    /// `unknown_timezone`.
    UnknownTimezone,
}
impl CronProblem {
    /// The field the problem is about: `cron` or `timezone`.
    pub fn field(self) -> &'static str {
        match self {
            Self::Cron => "cron",
            Self::TimezoneShape | Self::UnknownTimezone => "timezone",
        }
    }
    /// The stable error code.
    pub fn code(self) -> &'static str {
        match self {
            Self::Cron => "invalid_cron",
            Self::TimezoneShape => "invalid_timezone",
            Self::UnknownTimezone => "unknown_timezone",
        }
    }
}

/// Five fields of the characters cron uses, at most [`MAX_CRON_LEN`] bytes.
/// Six fields (seconds) and `@` nicknames are refused.
pub fn valid_cron_shape(cron: &str) -> bool {
    cron.len() <= MAX_CRON_LEN
        && cron.split_whitespace().count() == 5
        && cron
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b" */,-?#LW".contains(&b))
}
/// A zone name's shape: a letter, then letters, digits and `_+-/`, at most
/// [`MAX_TIMEZONE_LEN`] bytes.
pub fn valid_timezone_shape(zone: &str) -> bool {
    (1..=MAX_TIMEZONE_LEN).contains(&zone.len())
        && zone.as_bytes()[0].is_ascii_alphabetic()
        && zone
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_+-/".contains(&b))
}

/// A parsed expression in its zone.
#[derive(Clone)]
pub struct CronSchedule {
    cron: Cron,
    zone: TimeZone,
    expression: String,
    timezone: String,
}
impl std::fmt::Debug for CronSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CronSchedule")
            .field("cron", &self.expression)
            .field("timezone", &self.timezone)
            .finish()
    }
}
impl CronSchedule {
    /// Parse `cron` in `timezone`, listing every problem (at most one per
    /// field). An expression that never fires from now on is refused.
    pub fn parse(cron: &str, timezone: &str) -> Result<Self, Vec<CronProblem>> {
        let mut problems = Vec::new();
        let parsed = if valid_cron_shape(cron) {
            CronParser::builder()
                .seconds(Seconds::Disallowed)
                .build()
                .parse(cron)
                .ok()
        } else {
            None
        };
        if parsed.is_none() {
            problems.push(CronProblem::Cron);
        }
        let zone = if !valid_timezone_shape(timezone) {
            problems.push(CronProblem::TimezoneShape);
            None
        } else {
            let zone = TimeZone::get(timezone).ok();
            if zone.is_none() {
                problems.push(CronProblem::UnknownTimezone);
            }
            zone
        };
        let (Some(cron_parsed), Some(zone)) = (parsed, zone) else {
            return Err(problems);
        };
        let schedule = Self {
            cron: cron_parsed,
            zone,
            expression: cron.to_owned(),
            timezone: timezone.to_owned(),
        };
        if schedule.next_after(Timestamp::now()).is_err() {
            return Err(vec![CronProblem::Cron]);
        }
        Ok(schedule)
    }
    /// The expression as declared.
    pub fn expression(&self) -> &str {
        &self.expression
    }
    /// The zone name as declared.
    pub fn timezone(&self) -> &str {
        &self.timezone
    }
    /// The first occurrence strictly after `after`, as an instant. Fails
    /// permanent `invalid_cron` when croner finds none (its search stops
    /// before year 5000).
    pub fn next_after(&self, after: Timestamp) -> Result<Timestamp, Failure> {
        let start = after.to_zoned(self.zone.clone());
        let next = self
            .cron
            .find_next_occurrence(&start, false)
            .map_err(|_| Failure::permanent("invalid_cron"))?
            .timestamp();
        if next > after {
            Ok(next)
        } else {
            // Croner moves forward by whole seconds; a result at or before
            // `after` would mean it rounded back over a fraction.
            Err(Failure::permanent("invalid_cron"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> Timestamp {
        text.parse().unwrap()
    }
    fn paris(cron: &str) -> CronSchedule {
        CronSchedule::parse(cron, "Europe/Paris").unwrap()
    }
    /// The next `n` occurrences after `from`, each search starting from
    /// the instant the previous one returned.
    fn walk(schedule: &CronSchedule, from: &str, n: usize) -> Vec<String> {
        let mut at = from.parse::<Timestamp>().unwrap();
        (0..n)
            .map(|_| {
                at = schedule.next_after(at).unwrap();
                at.to_string()
            })
            .collect()
    }

    #[test]
    fn five_fields_only_and_every_problem_listed() {
        assert!(CronSchedule::parse("0 9 * * mon-fri", "Europe/Paris").is_ok());
        assert!(CronSchedule::parse("*/15 8-18 L * 1#2", "UTC").is_ok());
        for seconds in ["0 0 9 * * *", "*/10 * * * * *"] {
            assert_eq!(
                CronSchedule::parse(seconds, "UTC").unwrap_err(),
                vec![CronProblem::Cron],
                "{seconds}"
            );
        }
        for bad in [
            "",
            "@daily",
            "* * * *",
            "61 * * * *",
            "* 25 * * *",
            "* * 32 * *",
            "* * * 13 *",
            "every day",
            "0 0 30 2 *",
            &format!("{} * * * *", "1,".repeat(70)),
        ] {
            assert_eq!(
                CronSchedule::parse(bad, "UTC").unwrap_err(),
                vec![CronProblem::Cron],
                "{bad}"
            );
        }
        assert_eq!(
            CronSchedule::parse("* * * * * *", "Mars/Olympus").unwrap_err(),
            vec![CronProblem::Cron, CronProblem::UnknownTimezone]
        );
        assert_eq!(
            CronSchedule::parse("* * * * *", "../etc/passwd").unwrap_err(),
            vec![CronProblem::TimezoneShape]
        );
        assert_eq!(CronProblem::UnknownTimezone.code(), "unknown_timezone");
        assert_eq!(CronProblem::TimezoneShape.field(), "timezone");
    }

    #[test]
    fn occurrences_are_strictly_after_the_start_in_the_zone() {
        let reminders = paris("0 9 * * mon-fri");
        // Friday 2026-10-23 09:00 CEST is 07:00Z; the next is Monday.
        assert_eq!(
            walk(&reminders, "2026-10-23T07:00:00Z", 2),
            ["2026-10-26T08:00:00Z", "2026-10-27T08:00:00Z"]
        );
        assert_eq!(
            reminders.next_after(at("2026-10-23T06:59:59.5Z")).unwrap(),
            at("2026-10-23T07:00:00Z")
        );
        let utc = CronSchedule::parse("*/5 * * * *", "UTC").unwrap();
        assert_eq!(
            utc.next_after(at("2026-10-24T10:05:00.000001Z")).unwrap(),
            at("2026-10-24T10:10:00Z")
        );
    }

    /// AP-B3 test 2, spring forward: Europe/Paris jumps from 02:00 CET
    /// to 03:00 CEST on 2026-03-29. A fixed time inside the gap runs at the
    /// first valid minute after it; an interval schedule skips the missing
    /// occurrences.
    #[test]
    fn the_spring_forward_gap_follows_croner() {
        assert_eq!(
            walk(&paris("30 2 * * *"), "2026-03-28T12:00:00Z", 2),
            [
                "2026-03-29T01:00:00Z", // 03:00 CEST, right after the gap
                "2026-03-30T00:30:00Z", // 02:30 CEST
            ]
        );
        assert_eq!(
            walk(&paris("*/30 * * * *"), "2026-03-29T00:15:00Z", 3),
            [
                "2026-03-29T00:30:00Z", // 01:30 CET
                "2026-03-29T01:00:00Z", // 03:00 CEST: 02:00 and 02:30 never existed
                "2026-03-29T01:30:00Z", // 03:30 CEST
            ]
        );
    }

    /// AP-B3 test 2, fall back: Europe/Paris repeats 02:00 to 03:00 on
    /// 2026-10-25 (CEST, then CET). A fixed time runs once, in the first
    /// pass; an interval schedule runs in both passes.
    #[test]
    fn the_fall_back_overlap_follows_croner() {
        assert_eq!(
            walk(&paris("30 2 * * *"), "2026-10-24T12:00:00Z", 2),
            [
                "2026-10-25T00:30:00Z", // 02:30 CEST, the first pass only
                "2026-10-26T01:30:00Z", // 02:30 CET the next day
            ]
        );
        assert_eq!(
            walk(&paris("*/30 * * * *"), "2026-10-24T23:45:00Z", 5),
            [
                "2026-10-25T00:00:00Z", // 02:00 CEST
                "2026-10-25T00:30:00Z", // 02:30 CEST
                "2026-10-25T01:00:00Z", // 02:00 CET
                "2026-10-25T01:30:00Z", // 02:30 CET
                "2026-10-25T02:00:00Z", // 03:00 CET
            ]
        );
    }
}
