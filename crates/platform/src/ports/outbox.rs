//! The outbox (R1, R3, R4a): records appended inside the caller's
//! transaction, relayed to jobs after commit, and the failure
//! classification job handlers report.
use super::Source;
use crate::BoxFuture;
use merezha_domain::{Classification, Failure, Identity, compatible_json};
use merezha_postgres::Tx;
use serde_json::Value;
use std::{sync::Arc, time::Duration};

/// Longest outbox dedupe key, so derived job keys (`source:dedupe_key`)
/// stay under the 200-byte job key limit (R4a).
pub const MAX_DEDUPE_KEY_BYTES: usize = 160;
/// Most records in one [`Outbox::append`] call.
pub const MAX_RECORDS_PER_APPEND: usize = 256;
/// Largest encoded record payload.
pub const MAX_PAYLOAD_BYTES: usize = 65_536;

/// A stable key per logical effect: 1 to 160 bytes of `[A-Za-z0-9._:/-]`.
/// Constructing one validates it, so every outbox implementation refuses
/// longer keys at `append`, never at relay.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DedupeKey(String);
impl DedupeKey {
    /// Fails permanent `invalid_dedupe_key`.
    pub fn new(value: impl Into<String>) -> Result<Self, Failure> {
        let value = value.into();
        let valid = (1..=MAX_DEDUPE_KEY_BYTES).contains(&value.len())
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b));
        if valid {
            Ok(Self(value))
        } else {
            Err(Failure::permanent("invalid_dedupe_key"))
        }
    }
    /// A key supplied over HTTP, namespaced by the calling principal as
    /// `api:{subject}:{key}` (R4a) so callers cannot collide.
    pub fn api(subject: &Identity, key: &str) -> Result<Self, Failure> {
        Self::new(format!("api:{}:{key}", subject.as_str()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Correlation and tenant carried from the request to the job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageContext {
    pub correlation_id: Identity,
    pub causation_id: Option<Identity>,
    pub tenant: Option<Identity>,
}

/// Where a record goes after commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxTarget {
    /// An integration event fanned out to subscriptions; the payload is the
    /// integration event envelope.
    Event { topic: Identity },
    /// One job on one queue.
    Job { queue: Identity, kind: Identity },
}

/// One outbox record. `Debug` never prints the payload.
#[derive(Clone, PartialEq)]
pub struct OutboxRecord {
    pub dedupe_key: DedupeKey,
    pub target: OutboxTarget,
    /// At most [`MAX_PAYLOAD_BYTES`] encoded, JSON-compatible numbers only.
    pub payload: Value,
    /// R3: the payload is nulled when the relay marks the record relayed,
    /// and jobs created from it null theirs at their terminal state.
    pub contains_secret: bool,
    pub run_at: Option<jiff::Timestamp>,
    pub priority: i16,
    pub context: MessageContext,
}
impl std::fmt::Debug for OutboxRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutboxRecord")
            .field("dedupe_key", &self.dedupe_key)
            .field("target", &self.target)
            .field("contains_secret", &self.contains_secret)
            .field("run_at", &self.run_at)
            .field("priority", &self.priority)
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}
impl OutboxRecord {
    /// Fails permanent `outbox_payload_limit` or `invalid_number`.
    pub fn validate(&self) -> Result<(), Failure> {
        compatible_json(&self.payload)?;
        let size = serde_json::to_vec(&self.payload)
            .map_err(|_| Failure::permanent("outbox_payload_limit"))?
            .len();
        if size > MAX_PAYLOAD_BYTES {
            return Err(Failure::permanent("outbox_payload_limit"));
        }
        Ok(())
    }
}
/// What [`Outbox::append`] checks before any write: at most
/// [`MAX_RECORDS_PER_APPEND`] records (`outbox_batch_limit`), each valid.
pub fn validate_append(records: &[OutboxRecord]) -> Result<(), Failure> {
    if records.len() > MAX_RECORDS_PER_APPEND {
        return Err(Failure::permanent("outbox_batch_limit"));
    }
    records.iter().try_for_each(OutboxRecord::validate)
}

/// What a duplicate `(source, dedupe_key)` does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnDuplicate {
    /// Skip it and count it in [`Appended::duplicates`].
    Ignore,
    /// Fail the call with conflict `outbox_duplicate`; the caller's
    /// transaction is expected to roll back.
    Fail,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Appended {
    pub inserted: usize,
    pub duplicates: usize,
}

/// Appends records inside the caller's open transaction (AP §B.3).
///
/// Contract: call [`validate_append`] first; never commit, never open a
/// transaction; issue `NOTIFY merezha_jobs_outbox`, delivered on commit;
/// store the current span's trace context. Deduplicate on
/// `(source, dedupe_key)` per `on_duplicate`. Attempts, backoff and
/// concurrency come from `[jobs.queues.<name>]` at relay time, not from
/// records.
pub trait Outbox: Send + Sync {
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        source: &'a str,
        records: &'a [OutboxRecord],
        on_duplicate: OnDuplicate,
    ) -> BoxFuture<'a, Result<Appended, Failure>>;
}

/// The outbox bound to one source, as [`crate::AppContext::outbox`] hands
/// it out. It validates every call before the implementation sees it.
#[derive(Clone)]
pub struct SourceOutbox {
    source: Source,
    inner: Arc<dyn Outbox>,
}
impl std::fmt::Debug for SourceOutbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceOutbox")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}
impl SourceOutbox {
    pub fn new(source: Source, inner: Arc<dyn Outbox>) -> Self {
        Self { source, inner }
    }
    pub fn source(&self) -> &Source {
        &self.source
    }
    pub async fn append(
        &self,
        tx: &mut Tx<'_>,
        records: &[OutboxRecord],
        on_duplicate: OnDuplicate,
    ) -> Result<Appended, Failure> {
        validate_append(records)?;
        if records.is_empty() {
            return Ok(Appended::default());
        }
        self.inner
            .append(tx, self.source.as_str(), records, on_duplicate)
            .await
    }
}

/// Fallback until the jobs runtime exists: refuses with permanent
/// `outbox_unavailable`, so the caller's transaction rolls back instead of
/// losing the record.
#[derive(Debug, Default)]
pub struct UnavailableOutbox;
impl Outbox for UnavailableOutbox {
    fn append<'a>(
        &'a self,
        _: &'a mut Tx<'_>,
        _: &'a str,
        _: &'a [OutboxRecord],
        _: OnDuplicate,
    ) -> BoxFuture<'a, Result<Appended, Failure>> {
        Box::pin(async { Err(Failure::permanent("outbox_unavailable")) })
    }
}

/// A job handler's failure. The classification drives retry (AP §B.6):
/// `permanent` dead-letters at once, `transient` and `conflict` retry with
/// backoff until `max_attempts`, waiting at least `retry_after` (capped by
/// the queue's `backoff_max_ms`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobFailure {
    pub code: String,
    pub classification: Classification,
    pub retry_after: Option<Duration>,
}
impl JobFailure {
    pub fn permanent(code: &str) -> Self {
        Failure::permanent(code).into()
    }
    pub fn transient(code: &str, retry_after: Option<Duration>) -> Self {
        Self {
            retry_after,
            ..Failure::transient(code).into()
        }
    }
    pub fn is_retryable(&self) -> bool {
        self.classification != Classification::Permanent
    }
}
impl From<Failure> for JobFailure {
    fn from(failure: Failure) -> Self {
        Self {
            code: failure.code,
            classification: failure.classification,
            retry_after: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: &str, payload: Value) -> OutboxRecord {
        OutboxRecord {
            dedupe_key: DedupeKey::new(key).unwrap(),
            target: OutboxTarget::Job {
                queue: Identity::new("notify.email").unwrap(),
                kind: Identity::new("notify.deliver").unwrap(),
            },
            payload,
            contains_secret: true,
            run_at: None,
            priority: 0,
            context: MessageContext {
                correlation_id: Identity::new("c1").unwrap(),
                causation_id: None,
                tenant: None,
            },
        }
    }

    #[test]
    fn dedupe_keys_are_bounded_and_api_keys_are_namespaced() {
        assert!(DedupeKey::new("a".repeat(160)).is_ok());
        for bad in [
            "".to_owned(),
            "a".repeat(161),
            "has space".into(),
            "é".into(),
        ] {
            assert_eq!(DedupeKey::new(bad).unwrap_err().code, "invalid_dedupe_key");
        }
        let subject = Identity::new("usr_01").unwrap();
        assert_eq!(
            DedupeKey::api(&subject, "order-7").unwrap().as_str(),
            "api:usr_01:order-7"
        );
        assert!(DedupeKey::api(&subject, &"k".repeat(150)).is_err());
    }

    #[test]
    fn appends_are_bounded_and_debug_hides_payloads() {
        let secret = record(
            "identity.magic_link:tok_1",
            serde_json::json!({"link": "https://x/t0ps3cret"}),
        );
        assert!(!format!("{secret:?}").contains("t0ps3cret"));
        assert!(validate_append(std::slice::from_ref(&secret)).is_ok());
        let many = vec![secret.clone(); MAX_RECORDS_PER_APPEND + 1];
        assert_eq!(
            validate_append(&many).unwrap_err().code,
            "outbox_batch_limit"
        );
        let big = record("k", Value::String("x".repeat(MAX_PAYLOAD_BYTES)));
        assert_eq!(big.validate().unwrap_err().code, "outbox_payload_limit");
        let unsafe_number = record("k", serde_json::json!({"n": 9_007_199_254_740_993u64}));
        assert_eq!(unsafe_number.validate().unwrap_err().code, "invalid_number");
    }

    #[test]
    fn job_failures_keep_the_classification() {
        let failure: JobFailure = Failure::conflict("database_conflict").into();
        assert!(failure.is_retryable());
        assert!(!JobFailure::permanent("invalid_job_payload").is_retryable());
        let later = JobFailure::transient("provider_rate_limited", Some(Duration::from_secs(30)));
        assert_eq!(later.classification, Classification::Transient);
        assert_eq!(later.retry_after, Some(Duration::from_secs(30)));
    }
}
