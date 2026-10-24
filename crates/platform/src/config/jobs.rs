//! `[jobs]` and the `[application]` extensions (AP §D.2). The keys exist
//! and validate in the kernel; the jobs runtime and the PostgreSQL
//! application core give them behaviour in later slices.
use super::{ConfigIssue, Layers, Origin, reader};
use std::{collections::BTreeMap, time::Duration};

const fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}
const DAY: u64 = 86_400_000;

/// Queue names: dot-separated `[a-z][a-z0-9_]*` segments, at most 64 bytes,
/// such as `default` or `notify.email`.
pub fn valid_queue_name(name: &str) -> bool {
    name.len() <= 64
        && name.split('.').all(|segment| {
            segment
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_lowercase)
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        })
}

/// Who runs projection catch-up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatchUp {
    /// In the background: on wake, notification and interval.
    Auto,
    /// Only through the admin endpoints.
    Manual,
}
/// `[application.projections]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationProjections {
    pub catch_up: CatchUp,
    /// 100 ms to 10 min, default 5 s.
    pub interval: Duration,
    /// Per background run, 1 s to 5 min, default 30 s.
    pub run_timeout: Duration,
    pub warm_readiness: bool,
}
/// `[application.reactors]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationReactors {
    /// 100 ms to 10 min, default 5 s.
    pub interval: Duration,
    /// 100 ms to 10 min, default 1 s; at most `retry_max`.
    pub retry_base: Duration,
    /// 1 s to 1 day, default 5 min.
    pub retry_max: Duration,
}
/// `[application.background]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationBackground {
    /// 1 to 16, default 2; shared by projection, rebuild and reactor runs.
    pub max_concurrent: u32,
}
/// `[application.append]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationAppend {
    /// 10 ms to 30 s, default 5 s.
    pub lock_timeout_max: Duration,
}

pub(super) struct ApplicationExtensions {
    pub projections: ApplicationProjections,
    pub reactors: ApplicationReactors,
    pub background: ApplicationBackground,
    pub append: ApplicationAppend,
}
pub(super) fn application_extensions(
    layers: &mut Layers,
    issues: &mut Vec<ConfigIssue>,
) -> ApplicationExtensions {
    let (mut r, _) = reader(layers, "application.projections", issues);
    let catch_up = r.choice(
        "catch_up",
        CatchUp::Auto,
        &[("auto", CatchUp::Auto), ("manual", CatchUp::Manual)],
    );
    let projections = ApplicationProjections {
        catch_up,
        interval: r.duration("interval_ms", ms(5_000), (ms(100), ms(600_000))),
        run_timeout: r.duration("run_timeout_ms", ms(30_000), (ms(1_000), ms(300_000))),
        warm_readiness: r.boolean("warm_readiness", true),
    };
    r.finish();
    let (mut r, _) = reader(layers, "application.reactors", issues);
    let interval = r.duration("interval_ms", ms(5_000), (ms(100), ms(600_000)));
    let base_origin = r.origin_of("retry_base_ms");
    let retry_base = r.duration("retry_base_ms", ms(1_000), (ms(100), ms(600_000)));
    let retry_max = r.duration("retry_max_ms", ms(300_000), (ms(1_000), ms(DAY)));
    if retry_base > retry_max {
        r.issue(
            "retry_base_ms",
            "retry_base_exceeds_max",
            base_origin.unwrap_or(Origin::Default),
        );
    }
    let reactors = ApplicationReactors {
        interval,
        retry_base,
        retry_max,
    };
    r.finish();
    let (mut r, _) = reader(layers, "application.background", issues);
    let background = ApplicationBackground {
        max_concurrent: r.integer("max_concurrent", 2, 1, 16) as u32,
    };
    r.finish();
    let (mut r, _) = reader(layers, "application.append", issues);
    let append = ApplicationAppend {
        lock_timeout_max: r.duration("lock_timeout_max_ms", ms(5_000), (ms(10), ms(30_000))),
    };
    r.finish();
    ApplicationExtensions {
        projections,
        reactors,
        background,
        append,
    }
}

/// `[jobs.relay]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelayConfig {
    /// 100 ms to 10 min, default 1 s.
    pub interval: Duration,
    /// 1 to 1000 records per pass, default 100.
    pub batch: u32,
}
/// `[jobs.defaults]`, and the effective settings of a queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobDefaults {
    /// 1 to 100, default 10.
    pub max_attempts: u32,
    /// 10 ms to 1 day, default 1 s; at most `backoff_max`.
    pub backoff_base: Duration,
    /// 10 ms to 1 day, default 1 h.
    pub backoff_max: Duration,
    /// 1 s to 10 min, default 30 s; heartbeats every third of it.
    pub lease: Duration,
    /// 1 s to 15 min, default 30 s.
    pub timeout: Duration,
}
impl Default for JobDefaults {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            backoff_base: ms(1_000),
            backoff_max: ms(3_600_000),
            lease: ms(30_000),
            timeout: ms(30_000),
        }
    }
}
/// `[jobs.queues.<name>]`: concurrency plus the defaults it overrides.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueConfig {
    /// Per instance, 1 to 256, default 2; at most 256 across queues.
    pub concurrency: u32,
    /// Across instances; 0 (the default) is unlimited.
    pub global_concurrency: u32,
    /// [`JobsConfig::defaults`] with this queue's overrides applied.
    pub settings: JobDefaults,
}
/// `[jobs.retention]`: string durations from 1 h to 3650 d.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobRetention {
    pub succeeded: Duration,
    pub dead: Duration,
    pub outbox: Duration,
    pub audit: Duration,
}
/// One `[[jobs.schedules]]` entry. The cron expression and time zone are
/// checked for shape here; the scheduler parses them when it starts.
#[derive(Clone, Debug, PartialEq)]
pub struct JobSchedule {
    pub name: String,
    pub cron: String,
    pub timezone: String,
    pub queue: String,
    pub kind: String,
    pub payload: serde_json::Value,
}
/// `[jobs]` (AP §D.2). There is no `enabled` key: jobs run whenever the
/// database is configured.
#[derive(Clone, Debug, PartialEq)]
pub struct JobsConfig {
    /// `false`: enqueue, relay and outbox only (an "api" role).
    pub workers: bool,
    /// 100 ms to 1 min, default 1 s.
    pub poll_interval: Duration,
    /// 100 ms to 10 min, default 5 s.
    pub reaper_interval: Duration,
    /// 0 to 10 min, default 20 s.
    pub drain_timeout: Duration,
    pub relay: RelayConfig,
    /// `[jobs.scheduler] interval_ms`: 1 s to 1 h, default 10 s.
    pub scheduler_interval: Duration,
    pub defaults: JobDefaults,
    pub queues: BTreeMap<String, QueueConfig>,
    pub retention: JobRetention,
    /// `[jobs.readiness] max_lag_ms`; unset, backlog is no readiness input.
    pub readiness_max_lag: Option<Duration>,
    pub schedules: Vec<JobSchedule>,
}

/// Most queues and schedules.
const MAX_QUEUES: usize = 64;
const MAX_SCHEDULES: usize = 256;
const MAX_TOTAL_CONCURRENCY: u64 = 256;
const MAX_SCHEDULE_PAYLOAD: usize = 65_536;

fn job_settings(r: &mut super::Reader<'_>, base: &JobDefaults) -> JobDefaults {
    let base_origin = r.origin_of("backoff_base_ms");
    let settings = JobDefaults {
        max_attempts: r.integer("max_attempts", u64::from(base.max_attempts), 1, 100) as u32,
        backoff_base: r.duration("backoff_base_ms", base.backoff_base, (ms(10), ms(DAY))),
        backoff_max: r.duration("backoff_max_ms", base.backoff_max, (ms(10), ms(DAY))),
        lease: r.duration("lease_ms", base.lease, (ms(1_000), ms(600_000))),
        timeout: r.duration("timeout_ms", base.timeout, (ms(1_000), ms(900_000))),
    };
    if settings.backoff_base > settings.backoff_max {
        r.issue(
            "backoff_base_ms",
            "backoff_base_exceeds_max",
            base_origin.unwrap_or(Origin::Default),
        );
    }
    settings
}

pub(super) fn jobs(layers: &mut Layers, issues: &mut Vec<ConfigIssue>) -> JobsConfig {
    let (mut r, _) = reader(layers, "jobs", issues);
    let workers = r.boolean("workers", true);
    let poll_interval = r.duration("poll_interval_ms", ms(1_000), (ms(100), ms(60_000)));
    let reaper_interval = r.duration("reaper_interval_ms", ms(5_000), (ms(100), ms(600_000)));
    let drain_timeout = r.duration("drain_timeout_ms", ms(20_000), (ms(0), ms(600_000)));
    let schedules = r.take("schedules");
    r.finish();
    let (mut r, _) = reader(layers, "jobs.relay", issues);
    let relay = RelayConfig {
        interval: r.duration("interval_ms", ms(1_000), (ms(100), ms(600_000))),
        batch: r.integer("batch", 100, 1, 1_000) as u32,
    };
    r.finish();
    let (mut r, _) = reader(layers, "jobs.scheduler", issues);
    let scheduler_interval = r.duration("interval_ms", ms(10_000), (ms(1_000), ms(3_600_000)));
    r.finish();
    let (mut r, _) = reader(layers, "jobs.defaults", issues);
    let defaults = job_settings(&mut r, &JobDefaults::default());
    r.finish();

    let (r, _) = reader(layers, "jobs.queues", issues);
    r.finish();
    let names: Vec<String> = layers
        .sections
        .keys()
        .filter_map(|k| k.strip_prefix("jobs.queues.").map(str::to_owned))
        .collect();
    if names.len() > MAX_QUEUES {
        issues.push(ConfigIssue::new(
            "jobs.queues",
            "too_many_items",
            Origin::File,
        ));
    }
    let mut queues = BTreeMap::new();
    let mut total = 0;
    let mut total_origin = Origin::Default;
    for name in names {
        let prefix = format!("jobs.queues.{name}");
        let (mut r, _) = reader(layers, &prefix, issues);
        if let Some(origin) = r.origin_of("concurrency") {
            total_origin = origin;
        }
        let concurrency = r.integer("concurrency", 2, 1, MAX_TOTAL_CONCURRENCY) as u32;
        let global_concurrency = r.integer("global_concurrency", 0, 0, 65_535) as u32;
        let settings = job_settings(&mut r, &defaults);
        r.finish();
        total += u64::from(concurrency);
        queues.insert(
            name,
            QueueConfig {
                concurrency,
                global_concurrency,
                settings,
            },
        );
    }
    if total > MAX_TOTAL_CONCURRENCY {
        issues.push(ConfigIssue::new(
            "jobs.queues",
            "concurrency_total_exceeded",
            total_origin,
        ));
    }

    let (mut r, _) = reader(layers, "jobs.retention", issues);
    let keep = (ms(3_600_000), ms(3_650 * DAY));
    let retention = JobRetention {
        succeeded: r.duration("succeeded", ms(7 * DAY), keep),
        dead: r.duration("dead", ms(30 * DAY), keep),
        outbox: r.duration("outbox", ms(7 * DAY), keep),
        audit: r.duration("audit", ms(365 * DAY), keep),
    };
    r.finish();
    let (mut r, _) = reader(layers, "jobs.readiness", issues);
    let readiness_max_lag = r.optional_duration("max_lag_ms", (ms(1_000), ms(DAY)));
    r.finish();

    let schedules = match schedules {
        None => Vec::new(),
        Some(entry) => read_schedules(entry.value, entry.origin, issues),
    };
    JobsConfig {
        workers,
        poll_interval,
        reaper_interval,
        drain_timeout,
        relay,
        scheduler_interval,
        defaults,
        queues,
        retention,
        readiness_max_lag,
        schedules,
    }
}

fn valid_cron(cron: &str) -> bool {
    let fields = cron.split_whitespace().count();
    cron.len() <= 128
        && (5..=6).contains(&fields)
        && cron
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b" */,-?#LW".contains(&b))
}
fn valid_timezone(zone: &str) -> bool {
    (1..=64).contains(&zone.len())
        && zone.as_bytes()[0].is_ascii_alphabetic()
        && zone
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_+-/".contains(&b))
}

fn read_schedules(
    value: toml::Value,
    origin: Origin,
    issues: &mut Vec<ConfigIssue>,
) -> Vec<JobSchedule> {
    let toml::Value::Array(items) = value else {
        issues.push(ConfigIssue::new(
            "jobs.schedules",
            "expected_table_array",
            origin,
        ));
        return Vec::new();
    };
    if items.len() > MAX_SCHEDULES {
        issues.push(ConfigIssue::new("jobs.schedules", "too_many_items", origin));
        return Vec::new();
    }
    let mut schedules = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (index, item) in items.into_iter().enumerate() {
        let key = |field: &str| format!("jobs.schedules[{index}].{field}");
        let toml::Value::Table(mut table) = item else {
            issues.push(ConfigIssue::new(
                format!("jobs.schedules[{index}]"),
                "expected_table",
                origin.clone(),
            ));
            continue;
        };
        let has_timezone = table.contains_key("timezone");
        let mut text = |field: &str, valid: &dyn Fn(&str) -> bool, code: &'static str| match table
            .remove(field)
        {
            Some(toml::Value::String(value)) if valid(&value) => Some(value),
            Some(toml::Value::String(_)) => {
                issues.push(ConfigIssue::new(key(field), code, origin.clone()));
                None
            }
            Some(_) => {
                issues.push(ConfigIssue::new(
                    key(field),
                    "expected_string",
                    origin.clone(),
                ));
                None
            }
            None => {
                issues.push(ConfigIssue::new(
                    key(field),
                    "missing_required",
                    origin.clone(),
                ));
                None
            }
        };
        let name = text("name", &|v| merezha_domain::valid_name(v), "invalid_name");
        let cron = text("cron", &valid_cron, "invalid_cron");
        let timezone = match has_timezone {
            true => text("timezone", &valid_timezone, "invalid_timezone"),
            false => Some("UTC".to_owned()),
        };
        let queue = text("queue", &valid_queue_name, "invalid_queue_name");
        let kind = text("kind", &valid_queue_name, "invalid_kind");
        let payload = match table.remove("payload") {
            None => Some(serde_json::Value::Object(Default::default())),
            Some(value) => json_payload(value).or_else(|| {
                issues.push(ConfigIssue::new(
                    key("payload"),
                    "invalid_payload",
                    origin.clone(),
                ));
                None
            }),
        };
        for unknown in table.keys() {
            issues.push(ConfigIssue::new(
                key(unknown),
                "unknown_key",
                origin.clone(),
            ));
        }
        if let Some(name) = &name
            && !seen.insert(name.clone())
        {
            issues.push(ConfigIssue::new(
                key("name"),
                "duplicate_name",
                origin.clone(),
            ));
        }
        if let (Some(name), Some(cron), Some(timezone), Some(queue), Some(kind), Some(payload)) =
            (name, cron, timezone, queue, kind, payload)
        {
            schedules.push(JobSchedule {
                name,
                cron,
                timezone,
                queue,
                kind,
                payload,
            });
        }
    }
    schedules
}

/// A TOML payload as JSON: no dates, at most 64 KiB encoded.
fn json_payload(value: toml::Value) -> Option<serde_json::Value> {
    fn convert(value: toml::Value) -> Option<serde_json::Value> {
        Some(match value {
            toml::Value::String(s) => serde_json::Value::String(s),
            toml::Value::Integer(i) => serde_json::Value::from(i),
            toml::Value::Float(f) => {
                serde_json::Number::from_f64(f).map(serde_json::Value::Number)?
            }
            toml::Value::Boolean(b) => serde_json::Value::Bool(b),
            toml::Value::Datetime(_) => return None,
            toml::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(convert).collect::<Option<_>>()?)
            }
            toml::Value::Table(table) => serde_json::Value::Object(
                table
                    .into_iter()
                    .map(|(k, v)| Some((k, convert(v)?)))
                    .collect::<Option<_>>()?,
            ),
        })
    }
    let json = convert(value)?;
    merezha_domain::compatible_json(&json).ok()?;
    (serde_json::to_vec(&json).ok()?.len() <= MAX_SCHEDULE_PAYLOAD).then_some(json)
}
