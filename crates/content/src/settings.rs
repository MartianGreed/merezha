//! The `[modules.content]` settings that entries need (section 9):
//! `limits.*` for request, entry, page, filter, sort, batch and query
//! bounds, `revisions.retain`, and the trash's `retention` and
//! `purge_cron` (C4). Keys of later slices stay `unknown_key` until the
//! slice that gives them behaviour.
use merezha_platform::{
    ModuleSection,
    config::{ConfigIssue, parse_duration},
};
use std::time::Duration;

/// The request, page and query bounds of the management API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Largest request body, 1024 to 8388608 bytes (uploads excepted).
    pub max_request_bytes: usize,
    /// Largest canonical JSON of one variant's fields, 1024 to 1048576.
    pub max_entry_bytes: usize,
    /// `limit` when a list names none, 1 to `max_page_size`.
    pub default_page_size: u32,
    /// Largest `limit`, 1 to 500.
    pub max_page_size: u32,
    /// Most filter clauses per list, 1 to 32.
    pub max_filters: u32,
    /// Most `sort` keys, 1 to 4.
    pub max_sort_keys: u32,
    /// Most items of one batch publish or unpublish, 1 to 500.
    pub max_batch: u32,
    /// Bound on every statement of a request, 100 ms to 30 s.
    pub query_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_request_bytes: 1_048_576,
            max_entry_bytes: 262_144,
            default_page_size: 25,
            max_page_size: 100,
            max_filters: 10,
            max_sort_keys: 2,
            max_batch: 100,
            query_timeout: Duration::from_secs(5),
        }
    }
}

/// `[modules.content.trash]`: how long trashed entries stay and when the
/// purge job runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trash {
    /// Entries trashed longer than this are purged, 1 day to 3650 days.
    pub retention: Duration,
    /// The `content.trash_purge` schedule, five cron fields in UTC
    /// (`PURGE_TIMEZONE`), declared through `JobsHandle::schedule`.
    pub purge_cron: String,
}
/// `trash.retention` when the key is absent: 30 days.
pub const DEFAULT_TRASH_RETENTION: Duration = Duration::from_secs(30 * 86_400);
/// `trash.purge_cron` when the key is absent.
pub const DEFAULT_PURGE_CRON: &str = "23 3 * * *";
impl Default for Trash {
    fn default() -> Self {
        Self {
            retention: DEFAULT_TRASH_RETENTION,
            purge_cron: DEFAULT_PURGE_CRON.to_owned(),
        }
    }
}

/// The settings of the content block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContentSettings {
    pub limits: Limits,
    /// Revisions kept per variant besides the published one, 2 to 10000.
    pub revisions_retain: u32,
    pub trash: Trash,
}
impl Default for ContentSettings {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            revisions_retain: 100,
            trash: Trash::default(),
        }
    }
}

/// The top-level keys `[modules.content]` accepts.
pub(crate) const KEYS: [&str; 4] = ["model", "limits", "revisions", "trash"];
const TRASH_KEYS: [&str; 2] = ["retention", "purge_cron"];
const LIMIT_KEYS: [&str; 8] = [
    "max_request_bytes",
    "max_entry_bytes",
    "default_page_size",
    "max_page_size",
    "max_filters",
    "max_sort_keys",
    "max_batch",
    "query_timeout",
];
const REVISION_KEYS: [&str; 1] = ["retain"];

impl ContentSettings {
    /// Read `limits` and `revisions` from the section, reporting every
    /// problem at once.
    pub fn parse(section: &ModuleSection) -> Result<Self, Vec<ConfigIssue>> {
        let mut issues = Vec::new();
        let mut settings = Self::default();
        let defaults = Limits::default();
        if let Some(table) = subtable(section, "limits", &LIMIT_KEYS, &mut issues) {
            let mut int = |key: &str, default: u64, min: u64, max: u64| {
                integer(
                    section,
                    "limits",
                    &table,
                    key,
                    default,
                    (min, max),
                    &mut issues,
                )
            };
            let limits = &mut settings.limits;
            limits.max_request_bytes = int(
                "max_request_bytes",
                defaults.max_request_bytes as u64,
                1024,
                8_388_608,
            ) as usize;
            limits.max_entry_bytes = int(
                "max_entry_bytes",
                defaults.max_entry_bytes as u64,
                1024,
                1_048_576,
            ) as usize;
            limits.max_page_size =
                int("max_page_size", defaults.max_page_size.into(), 1, 500) as u32;
            limits.default_page_size = int(
                "default_page_size",
                defaults.default_page_size.min(limits.max_page_size).into(),
                1,
                limits.max_page_size.into(),
            ) as u32;
            limits.max_filters = int("max_filters", defaults.max_filters.into(), 1, 32) as u32;
            limits.max_sort_keys = int("max_sort_keys", defaults.max_sort_keys.into(), 1, 4) as u32;
            limits.max_batch = int("max_batch", defaults.max_batch.into(), 1, 500) as u32;
            if let Some(value) = table.get("query_timeout") {
                let key = format!("{}.query_timeout", section.key("limits"));
                match value.as_str().and_then(parse_duration) {
                    Some(d)
                        if (Duration::from_millis(100)..=Duration::from_secs(30)).contains(&d) =>
                    {
                        limits.query_timeout = d;
                    }
                    Some(_) => issues.push(ConfigIssue::new(
                        key,
                        "out_of_range",
                        section.origin("limits"),
                    )),
                    None => issues.push(ConfigIssue::new(
                        key,
                        "expected_duration",
                        section.origin("limits"),
                    )),
                }
            }
        }
        if let Some(table) = subtable(section, "revisions", &REVISION_KEYS, &mut issues) {
            settings.revisions_retain = integer(
                section,
                "revisions",
                &table,
                "retain",
                100,
                (2, 10_000),
                &mut issues,
            ) as u32;
        }
        if let Some(table) = subtable(section, "trash", &TRASH_KEYS, &mut issues) {
            let origin = section.origin("trash");
            if let Some(value) = table.get("retention") {
                let key = format!("{}.retention", section.key("trash"));
                let range = Duration::from_secs(86_400)..=Duration::from_secs(3650 * 86_400);
                match value.as_str().and_then(parse_duration) {
                    Some(d) if range.contains(&d) => settings.trash.retention = d,
                    Some(_) => issues.push(ConfigIssue::new(key, "out_of_range", origin.clone())),
                    None => issues.push(ConfigIssue::new(key, "expected_duration", origin.clone())),
                }
            }
            if let Some(value) = table.get("purge_cron") {
                let key = format!("{}.purge_cron", section.key("trash"));
                match value.as_str() {
                    Some(cron) if valid_cron(cron) => {
                        cron.clone_into(&mut settings.trash.purge_cron)
                    }
                    Some(_) => issues.push(ConfigIssue::new(key, "invalid_cron", origin)),
                    None => issues.push(ConfigIssue::new(key, "expected_string", origin)),
                }
            }
        }
        if issues.is_empty() {
            Ok(settings)
        } else {
            Err(issues)
        }
    }
}

/// The time zone of the `content.trash_purge` schedule.
pub const PURGE_TIMEZONE: &str = "UTC";

/// A five-field cron expression the jobs scheduler accepts in UTC, by
/// AP's parser (§B.7).
fn valid_cron(cron: &str) -> bool {
    merezha_platform::cron::CronSchedule::parse(cron, PURGE_TIMEZONE).is_ok()
}

fn subtable(
    section: &ModuleSection,
    name: &str,
    known: &[&str],
    issues: &mut Vec<ConfigIssue>,
) -> Option<toml::Table> {
    let origin = section.origin(name);
    match section.table.get(name)? {
        toml::Value::Table(table) => {
            for key in table.keys().filter(|k| !known.contains(&k.as_str())) {
                issues.push(ConfigIssue::new(
                    format!("{}.{key}", section.key(name)),
                    "unknown_key",
                    origin.clone(),
                ));
            }
            Some(table.clone())
        }
        _ => {
            issues.push(ConfigIssue::new(
                section.key(name),
                "expected_table",
                origin,
            ));
            None
        }
    }
}

fn integer(
    section: &ModuleSection,
    table_name: &str,
    table: &toml::Table,
    key: &str,
    default: u64,
    (min, max): (u64, u64),
    issues: &mut Vec<ConfigIssue>,
) -> u64 {
    let Some(value) = table.get(key) else {
        return default;
    };
    let dotted = format!("{}.{key}", section.key(table_name));
    let origin = section.origin(table_name);
    match value {
        toml::Value::Integer(n) => match u64::try_from(*n) {
            Ok(n) if (min..=max).contains(&n) => n,
            _ => {
                issues.push(ConfigIssue::new(dotted, "out_of_range", origin));
                default
            }
        },
        _ => {
            issues.push(ConfigIssue::new(dotted, "expected_integer", origin));
            default
        }
    }
}
