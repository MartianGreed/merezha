use crate::config::Limits;
use merezha_domain::{Event, Failure, Identity, Metadata, Receipt, StoredEvent, compatible_json};
use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

/// Replaceable, synchronous event-store port. Implementations must bound all
/// returned data and commit the entire batch atomically under expected version.
/// Positions start at one and are contiguous across streams. No partial writes
/// are allowed on any error. This interface alone gives no durability promise.
pub trait EventStore: Send + Sync {
    fn load(&self, stream: &Identity) -> Result<Vec<StoredEvent>, Failure>;
    fn head(&self) -> Result<u64, Failure>;
    fn read_all(&self, after: u64, limit: usize) -> Result<Vec<StoredEvent>, Failure>;
    fn append(
        &self,
        stream: &Identity,
        expected: u64,
        events: Vec<Event>,
        metadata: &Metadata,
    ) -> Result<Receipt, Failure>;
}
#[derive(Default)]
struct State {
    events: Vec<StoredEvent>,
    versions: BTreeMap<Identity, u64>,
    bytes: usize,
}
/// Finite process-local history. At capacity, writes fail rather than evicting
/// authoritative events. Restart discards all history.
pub struct MemoryEventStore {
    limits: Limits,
    state: Mutex<State>,
}
impl MemoryEventStore {
    pub fn new(limits: Limits) -> Result<Self, Failure> {
        limits.validate()?;
        Ok(Self {
            limits,
            state: Mutex::new(State::default()),
        })
    }
}
fn internal<T>(_: T) -> Failure {
    Failure::transient("store_unavailable")
}
impl EventStore for MemoryEventStore {
    fn load(&self, stream: &Identity) -> Result<Vec<StoredEvent>, Failure> {
        let state = self.state.lock().map_err(internal)?;
        Ok(state
            .events
            .iter()
            .filter(|event| &event.stream == stream)
            .cloned()
            .collect())
    }
    fn head(&self) -> Result<u64, Failure> {
        Ok(self.state.lock().map_err(internal)?.events.len() as u64)
    }
    fn read_all(&self, after: u64, limit: usize) -> Result<Vec<StoredEvent>, Failure> {
        if limit == 0 || limit > 64 {
            return Err(Failure::permanent("invalid_batch"));
        }
        let state = self.state.lock().map_err(internal)?;
        let skip = usize::try_from(after).unwrap_or(usize::MAX);
        Ok(state
            .events
            .iter()
            .skip(skip)
            .take(limit)
            .cloned()
            .collect())
    }
    fn append(
        &self,
        stream: &Identity,
        expected: u64,
        events: Vec<Event>,
        metadata: &Metadata,
    ) -> Result<Receipt, Failure> {
        if events.len() > self.limits.batch_size {
            return Err(Failure::permanent("event_batch_limit"));
        }
        let mut state = self.state.lock().map_err(internal)?;
        let version = state.versions.get(stream).copied().unwrap_or(0);
        if version != expected {
            return Err(Failure::conflict("concurrency_conflict"));
        }
        if events.len() > self.limits.max_events.saturating_sub(state.events.len())
            || (!events.is_empty()
                && !state.versions.contains_key(stream)
                && state.versions.len() >= self.limits.max_streams)
        {
            return Err(Failure::transient("store_capacity"));
        }
        let accepted_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut batch = Vec::with_capacity(events.len());
        let mut bytes = 0;
        for (index, event) in events.into_iter().enumerate() {
            if event.schema_version == 0 {
                return Err(Failure::permanent("invalid_event"));
            }
            compatible_json(&event.payload)?;
            if serde_json::to_vec(&event.payload).map_err(internal)?.len()
                > self.limits.max_payload_bytes
            {
                return Err(Failure::permanent("payload_limit"));
            }
            let position = state.events.len() as u64 + index as u64 + 1;
            let stored = StoredEvent {
                event_id: Identity::new(format!("event/{position}"))?,
                stream: stream.clone(),
                version: version + index as u64 + 1,
                position,
                event,
                metadata: metadata.clone(),
                accepted_unix_ms,
            };
            bytes += serde_json::to_vec(&stored).map_err(internal)?.len();
            batch.push(stored);
        }
        if bytes > self.limits.max_store_bytes.saturating_sub(state.bytes) {
            return Err(Failure::transient("store_capacity"));
        }
        let count = batch.len();
        let version = version + count as u64;
        if count > 0 {
            state.versions.insert(stream.clone(), version);
        }
        state.events.extend(batch);
        state.bytes += bytes;
        Ok(Receipt {
            stream: stream.clone(),
            version,
            position: state.events.len() as u64,
            events: count,
        })
    }
}
