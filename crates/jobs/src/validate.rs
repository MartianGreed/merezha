//! Checks run before anything is written, so the tables' CHECK constraints
//! never refuse a record at relay (R4a).
use merezha_domain::{Failure, compatible_json};
use merezha_platform::{
    config::valid_queue_name,
    ports::{IntegrationEvent, MAX_PAYLOAD_BYTES, OutboxRecord, OutboxTarget},
};
use serde_json::Value;

/// Delayed jobs run at most this far ahead (AP §B.7).
const MAX_DELAY_HOURS: i64 = 365 * 24;

/// Outbox sources: `[a-z][a-z0-9_]{0,31}`, the column's CHECK. `outbox` and
/// `cron` are refused: they prefix the keys the relay and the scheduler mint.
pub(crate) fn source(source: &str) -> Result<(), Failure> {
    let bytes = source.as_bytes();
    let valid = (1..=32).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
        && !matches!(source, "outbox" | "cron");
    if valid {
        Ok(())
    } else {
        Err(Failure::permanent("invalid_source"))
    }
}

fn segment(value: &str, max: usize) -> bool {
    let bytes = value.as_bytes();
    (1..=max).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}

/// Topics are `<module>.<name>` (R11): `[a-z][a-z0-9_]{0,31}` then
/// `[a-z][a-z0-9_]{0,63}`. Fails permanent `invalid_topic`.
pub(crate) fn topic(topic: &str) -> Result<(), Failure> {
    match topic.split_once('.') {
        Some((module, name)) if segment(module, 32) && segment(name, 64) => Ok(()),
        _ => Err(Failure::permanent("invalid_topic")),
    }
}

/// Queue names as `[jobs.queues.<name>]` accepts them. Fails permanent
/// `invalid_queue`.
pub(crate) fn queue(queue: &str) -> Result<(), Failure> {
    if valid_queue_name(queue) {
        Ok(())
    } else {
        Err(Failure::permanent("invalid_queue"))
    }
}

/// At most 365 days ahead. Fails permanent `invalid_run_at`.
pub(crate) fn run_at(at: Option<jiff::Timestamp>, now: jiff::Timestamp) -> Result<(), Failure> {
    let ceiling = now.saturating_add(jiff::SignedDuration::from_hours(MAX_DELAY_HOURS));
    match (at, ceiling) {
        (Some(at), Ok(ceiling)) if at > ceiling => Err(Failure::permanent("invalid_run_at")),
        _ => Ok(()),
    }
}

/// JSON-compatible numbers and at most 64 KiB encoded; returns the text
/// stored. Fails permanent `invalid_number` or `job_payload_limit`.
pub(crate) fn job_payload(payload: &Value) -> Result<String, Failure> {
    compatible_json(payload)?;
    let text =
        serde_json::to_string(payload).map_err(|_| Failure::permanent("job_payload_limit"))?;
    if text.len() > MAX_PAYLOAD_BYTES {
        return Err(Failure::permanent("job_payload_limit"));
    }
    Ok(text)
}

/// Everything about one record [`merezha_platform::ports::validate_append`]
/// cannot know: its target names, its delay, and for an event record the
/// envelope, which must be the one [`OutboxRecord::event`] builds for this
/// source.
pub(crate) fn record(
    source: &str,
    record: &OutboxRecord,
    now: jiff::Timestamp,
) -> Result<(), Failure> {
    run_at(record.run_at, now)?;
    match &record.target {
        OutboxTarget::Job { queue: name, .. } => queue(name.as_str()),
        OutboxTarget::Event { topic: name } => {
            topic(name.as_str())?;
            let envelope = IntegrationEvent::from_value(&record.payload)
                .map_err(|_| Failure::permanent("invalid_event_envelope"))?;
            let context = &record.context;
            let consistent = envelope.version >= 1
                && envelope.event_type == name.as_str()
                && envelope.source == source
                && envelope.event_id == format!("{source}:{}", record.dedupe_key.as_str())
                && envelope.tenant == context.tenant
                && envelope.correlation_id == context.correlation_id
                && envelope.causation_id == context.causation_id;
            if consistent {
                Ok(())
            } else {
                Err(Failure::permanent("invalid_event_envelope"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_column_checks() {
        for good in ["identity", "a", "tasks_2", &"s".repeat(32)] {
            assert!(source(good).is_ok(), "{good}");
        }
        for bad in [
            "",
            "Identity",
            "1a",
            "a-b",
            "outbox",
            "cron",
            &"s".repeat(33),
        ] {
            assert_eq!(source(bad).unwrap_err().code, "invalid_source", "{bad}");
        }
        assert!(topic("identity.user_registered").is_ok());
        for bad in [
            "identity",
            "identity.",
            ".x",
            "a.b.c",
            "Identity.x",
            "a.B",
            &format!("{}.x", "m".repeat(33)),
            &format!("m.{}", "x".repeat(65)),
        ] {
            assert_eq!(topic(bad).unwrap_err().code, "invalid_topic", "{bad}");
        }
        assert!(queue("notify.email").is_ok());
        for bad in ["Notify", "a-b", "a..b", ""] {
            assert_eq!(queue(bad).unwrap_err().code, "invalid_queue", "{bad}");
        }
    }

    #[test]
    fn delays_and_payloads_are_bounded() {
        let now = jiff::Timestamp::now();
        let day = jiff::SignedDuration::from_hours(24);
        assert!(run_at(Some(now.saturating_add(day * 365).unwrap()), now).is_ok());
        assert_eq!(
            run_at(Some(now.saturating_add(day * 366).unwrap()), now)
                .unwrap_err()
                .code,
            "invalid_run_at"
        );
        assert!(run_at(None, now).is_ok());
        assert_eq!(
            job_payload(&Value::String("x".repeat(MAX_PAYLOAD_BYTES)))
                .unwrap_err()
                .code,
            "job_payload_limit"
        );
        assert_eq!(
            job_payload(&serde_json::json!({"b": 1, "a": [true]})).unwrap(),
            r#"{"a":[true],"b":1}"#
        );
    }
}
