use crate::{
    Authorizer, Guest, GuestSession,
    config::Config,
    store::{EventStore, MemoryEventStore},
};
use merezha_domain::{
    Command, Failure, Identity, MessageKind, Operation, Query, Receipt, Reply, compatible_json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ProjectionState {
    pub state: Value,
    pub checkpoint: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct QueryResult {
    pub value: Value,
    pub checkpoint: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Key {
    pub(crate) tag: Identity,
    pub(crate) actor: Option<Identity>,
    pub(crate) key: Identity,
}
pub(crate) struct Record {
    pub(crate) payload: String,
    pub(crate) result: Option<Receipt>,
}
struct Inner {
    config: Config,
    guest: Arc<dyn Guest>,
    authorizer: Arc<dyn Authorizer>,
    store: Storage,
    idempotency: Mutex<BTreeMap<Key, Record>>,
    projections: BTreeMap<Identity, Mutex<ProjectionState>>,
    active: AtomicUsize,
}
enum Storage {
    Volatile(Arc<dyn EventStore>),
    Sqlite(Arc<crate::sqlite::SqliteStore>),
}
impl Storage {
    fn load(
        &self,
        stream: &Identity,
        deadline: std::time::Instant,
    ) -> Result<Vec<merezha_domain::StoredEvent>, Failure> {
        match self {
            Self::Volatile(s) => s.load(stream),
            Self::Sqlite(s) => s.load(stream, deadline),
        }
    }
    fn head(&self, deadline: std::time::Instant) -> Result<u64, Failure> {
        match self {
            Self::Volatile(s) => s.head(),
            Self::Sqlite(s) => s.head(deadline),
        }
    }
    fn read_all(
        &self,
        after: u64,
        limit: usize,
        deadline: std::time::Instant,
    ) -> Result<Vec<merezha_domain::StoredEvent>, Failure> {
        match self {
            Self::Volatile(s) => s.read_all(after, limit),
            Self::Sqlite(s) => s.read_all(after, limit, deadline),
        }
    }
}
/// Clones share dispatch state and, in SQLite mode, one owned database.
#[derive(Clone)]
pub struct Application(Arc<Inner>);
pub struct Admission {
    inner: Arc<Inner>,
    deadline: std::time::Instant,
}
impl Drop for Admission {
    fn drop(&mut self) {
        self.inner.active.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Claim {
    inner: Arc<Inner>,
    key: Key,
    fingerprint: String,
    completed: bool,
}
impl Drop for Claim {
    fn drop(&mut self) {
        if !self.completed {
            // No user or guest code executes while this map is locked.
            let mut records = self
                .inner
                .idempotency
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            records.remove(&self.key);
        }
    }
}
impl Claim {
    fn complete(mut self, receipt: &Receipt) {
        let mut records = self
            .inner
            .idempotency
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(record) = records.get_mut(&self.key) {
            record.result = Some(receipt.clone());
        }
        self.completed = true;
    }
}
impl Application {
    /// Construct the configured backend before accepting requests. SQLite
    /// requires an absolute local path and explicit initialize or verified open.
    pub fn open(
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Self, Failure> {
        use crate::config::StorageConfig;
        validate_services(&config, guest.as_ref())?;
        if let StorageConfig::Sqlite {
            path,
            application,
            initialize,
        } = &config.storage
        {
            let (store, (records, recovered)) =
                crate::sqlite::SqliteStore::open(&config, path, application, *initialize)?;
            Self::construct(
                config,
                guest,
                authorizer,
                Storage::Sqlite(Arc::new(store)),
                records,
                recovered,
            )
        } else {
            Self::in_memory(config, guest, authorizer)
        }
    }
    pub fn in_memory(
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Result<Self, Failure> {
        let store = Arc::new(MemoryEventStore::new(config.limits.clone())?);
        Self::new(config, guest, authorizer, store)
    }
    pub fn new(
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
        store: Arc<dyn EventStore>,
    ) -> Result<Self, Failure> {
        if !matches!(config.storage, crate::config::StorageConfig::Memory) {
            return Err(Failure::permanent("storage_configuration"));
        }
        Self::construct(
            config,
            guest,
            authorizer,
            Storage::Volatile(store),
            BTreeMap::new(),
            BTreeMap::new(),
        )
    }
    fn construct(
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
        store: Storage,
        records: BTreeMap<Key, Record>,
        mut recovered: BTreeMap<Identity, ProjectionState>,
    ) -> Result<Self, Failure> {
        validate_services(&config, guest.as_ref())?;
        let mut projections = BTreeMap::new();
        for projection in &config.projections {
            compatible_json(&projection.initial)?;
            projections.insert(
                projection.name.clone(),
                Mutex::new(
                    recovered
                        .remove(&projection.name)
                        .unwrap_or_else(|| ProjectionState {
                            state: projection.initial.clone(),
                            checkpoint: 0,
                        }),
                ),
            );
        }
        Ok(Self(Arc::new(Inner {
            config,
            guest,
            authorizer,
            store,
            projections,
            idempotency: Mutex::new(records),
            active: AtomicUsize::new(0),
        })))
    }
    /// Reserve before reading an HTTP body or submitting a blocking task.
    pub fn admit(&self) -> Result<Admission, Failure> {
        self.0
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.0.config.limits.max_concurrent).then_some(n + 1)
            })
            .map_err(|_| Failure::transient("overloaded"))?;
        Ok(Admission {
            inner: self.0.clone(),
            deadline: std::time::Instant::now()
                + std::time::Duration::from_millis(self.0.config.limits.timeout_ms),
        })
    }
    pub fn command(&self, command: Command) -> Result<Receipt, Failure> {
        self.admit()?.command(command)
    }
    pub fn query(&self, query: Query) -> Result<QueryResult, Failure> {
        self.admit()?.query(query)
    }
    pub fn catch_up(&self, name: &str) -> Result<ProjectionState, Failure> {
        self.admit()?.project(name, false)
    }
    pub fn rebuild(&self, name: &str) -> Result<ProjectionState, Failure> {
        self.admit()?.project(name, true)
    }
}
impl Admission {
    pub fn deadline(&self) -> std::time::Instant {
        self.deadline
    }
    fn validate(
        &self,
        session: &dyn GuestSession,
        kind: MessageKind,
        service: &str,
        tag: &Identity,
        payload: Value,
    ) -> Result<Value, Failure> {
        self.check_value(&payload, self.inner.config.limits.max_payload_bytes)?;
        match invoke(
            session,
            self.deadline,
            service,
            Operation::Validate {
                kind,
                tag: tag.clone(),
                payload,
            },
        )? {
            Reply::Validated { payload } => {
                self.check_value(&payload, self.inner.config.limits.max_payload_bytes)?;
                Ok(payload)
            }
            _ => Err(Failure::permanent("invalid_response")),
        }
    }
    fn check_value(&self, value: &Value, maximum: usize) -> Result<(), Failure> {
        compatible_json(value)?;
        if serde_json::to_vec(value)
            .map_err(|_| Failure::permanent("serialization_failed"))?
            .len()
            > maximum
        {
            return Err(Failure::permanent("payload_limit"));
        }
        Ok(())
    }
    pub fn command(self, command: Command) -> Result<Receipt, Failure> {
        let route = self
            .inner
            .config
            .commands
            .iter()
            .find(|r| r.tag == command.tag)
            .ok_or_else(|| Failure::permanent("handler_not_found"))?;
        let stream = Identity::new(format!(
            "{}/{}",
            route.aggregate.as_str(),
            command.stream.as_str()
        ))?;
        let session = self.inner.guest.session()?;
        let payload = self.validate(
            session.as_ref(),
            MessageKind::Command,
            route.service.as_str(),
            &command.tag,
            command.payload,
        )?;
        self.inner.authorizer.authorize(
            MessageKind::Command,
            command.tag.as_str(),
            Some(stream.as_str()),
            &payload,
            &command.metadata,
        )?;
        let mut claim = None;
        if let Some(key) = command.idempotency_key {
            let key = Key {
                tag: command.tag.clone(),
                actor: command.metadata.actor.clone(),
                key,
            };
            // Bind stream and explicit expected version too, so a key cannot
            // silently replay the result of another target or precondition.
            let fingerprint = canonical(
                &serde_json::json!({"stream":stream,"payload":payload,"expected_version":command.expected_version}),
            )?;
            let mut records = self
                .inner
                .idempotency
                .lock()
                .map_err(|_| Failure::transient("internal"))?;
            if let Some(record) = records.get(&key) {
                if record.payload != fingerprint {
                    return Err(Failure::conflict("idempotency_mismatch"));
                }
                return record
                    .result
                    .clone()
                    .ok_or_else(|| Failure::conflict("idempotency_in_flight"));
            }
            if records.len() >= self.inner.config.limits.max_idempotency {
                return Err(Failure::transient("idempotency_capacity"));
            }
            records.insert(
                key.clone(),
                Record {
                    payload: fingerprint.clone(),
                    result: None,
                },
            );
            claim = Some(Claim {
                inner: self.inner.clone(),
                key,
                fingerprint,
                completed: false,
            });
        }
        let history = self.inner.store.load(&stream, self.deadline)?;
        if history.len() > self.inner.config.limits.max_events {
            return Err(Failure::permanent("history_limit"));
        }
        let expected = history.last().map_or(0, |event| event.version);
        if command.expected_version.is_some_and(|v| v != expected) {
            return Err(Failure::conflict("concurrency_conflict"));
        }
        let reply = invoke(
            session.as_ref(),
            self.deadline,
            route.service.as_str(),
            Operation::Decide {
                tag: command.tag,
                stream: stream.clone(),
                payload,
                history,
                metadata: command.metadata.clone(),
            },
        )?;
        let Reply::Decided { events } = reply else {
            return Err(Failure::permanent("invalid_response"));
        };
        if std::time::Instant::now() >= self.deadline {
            return Err(Failure::transient("timeout"));
        }
        if events.len() > self.inner.config.limits.batch_size {
            return Err(Failure::permanent("event_batch_limit"));
        }
        let receipt = match &self.inner.store {
            Storage::Sqlite(store) => store.commit_command(
                &stream,
                expected,
                events,
                &command.metadata,
                claim.as_ref().map(|c| (&c.key, c.fingerprint.as_str())),
                self.deadline,
            )?,
            Storage::Volatile(store) => {
                store.append(&stream, expected, events, &command.metadata)?
            }
        };
        if let Some(claim) = claim {
            claim.complete(&receipt);
        }
        Ok(receipt)
    }
    pub fn query(self, query: Query) -> Result<QueryResult, Failure> {
        let route = self
            .inner
            .config
            .queries
            .iter()
            .find(|r| r.tag == query.tag)
            .ok_or_else(|| Failure::permanent("handler_not_found"))?;
        let session = self.inner.guest.session()?;
        let payload = self.validate(
            session.as_ref(),
            MessageKind::Query,
            route.service.as_str(),
            &query.tag,
            query.payload,
        )?;
        self.inner.authorizer.authorize(
            MessageKind::Query,
            query.tag.as_str(),
            None,
            &payload,
            &query.metadata,
        )?;
        let snapshot = match &self.inner.store {
            Storage::Sqlite(store) => store.projection(&route.projection, self.deadline)?.0,
            Storage::Volatile(_) => self.inner.projections[&route.projection]
                .try_lock()
                .map_err(|_| Failure::conflict("projection_in_flight"))?
                .clone(),
        };
        let reply = invoke(
            session.as_ref(),
            self.deadline,
            route.service.as_str(),
            Operation::Query {
                tag: query.tag,
                payload,
                state: snapshot.state,
                metadata: query.metadata,
            },
        )?;
        let Reply::Queried { value } = reply else {
            return Err(Failure::permanent("invalid_response"));
        };
        self.check_value(&value, self.inner.config.limits.max_state_bytes)?;
        Ok(QueryResult {
            value,
            checkpoint: snapshot.checkpoint,
        })
    }
    pub fn project(self, name: &str, rebuild: bool) -> Result<ProjectionState, Failure> {
        let projection = self
            .inner
            .config
            .projections
            .iter()
            .find(|p| p.name.as_str() == name)
            .ok_or_else(|| Failure::permanent("projection_not_found"))?;
        let session = self.inner.guest.session()?;
        let mut current = self.inner.projections[&projection.name]
            .try_lock()
            .map_err(|_| Failure::conflict("projection_in_flight"))?;
        let mut revision = 0;
        if let Storage::Sqlite(store) = &self.inner.store {
            let (snapshot, stored_revision) = store.projection(&projection.name, self.deadline)?;
            *current = snapshot;
            revision = stored_revision;
        }
        let mut pending = if rebuild {
            ProjectionState {
                state: projection.initial.clone(),
                checkpoint: 0,
            }
        } else {
            current.clone()
        };
        // Snapshot the target so continuous concurrent appends cannot keep a
        // catch-up request alive indefinitely.
        let head = self.inner.store.head(self.deadline)?;
        while pending.checkpoint < head {
            let mut events = self.inner.store.read_all(
                pending.checkpoint,
                self.inner.config.limits.batch_size,
                self.deadline,
            )?;
            events.retain(|event| event.position <= head);
            if events.is_empty() || events.len() > self.inner.config.limits.batch_size {
                return Err(Failure::permanent("invalid_event_feed"));
            }
            let mut checkpoint = pending.checkpoint;
            for event in &events {
                if event.position != checkpoint + 1 {
                    return Err(Failure::permanent("invalid_event_feed"));
                }
                checkpoint = event.position;
            }
            let reply = invoke(
                session.as_ref(),
                self.deadline,
                projection.service.as_str(),
                Operation::Project {
                    projection: projection.name.clone(),
                    state: pending.state.clone(),
                    events,
                    live: !rebuild,
                },
            )?;
            let Reply::Projected { state } = reply else {
                return Err(Failure::permanent("invalid_response"));
            };
            self.check_value(&state, self.inner.config.limits.max_state_bytes)?;
            pending = ProjectionState { state, checkpoint };
            if !rebuild {
                if let Storage::Sqlite(store) = &self.inner.store {
                    revision = store.commit_projection(
                        &projection.name,
                        revision,
                        &pending,
                        self.deadline,
                    )?;
                }
                *current = pending.clone();
            }
        }
        // Rebuild swaps its entire candidate only after every batch succeeds.
        if rebuild && let Storage::Sqlite(store) = &self.inner.store {
            store.commit_projection(&projection.name, revision, &pending, self.deadline)?;
        }
        *current = pending;
        Ok(current.clone())
    }
}
fn validate_services(config: &Config, guest: &dyn Guest) -> Result<(), Failure> {
    config.validate()?;
    let services = guest.services()?;
    if config
        .commands
        .iter()
        .map(|r| &r.service)
        .chain(config.queries.iter().map(|r| &r.service))
        .chain(config.projections.iter().map(|r| &r.service))
        .any(|service| !services.iter().any(|s| s == service.as_str()))
    {
        return Err(Failure::permanent("unknown_service"));
    }
    Ok(())
}
fn invoke(
    session: &dyn GuestSession,
    deadline: std::time::Instant,
    service: &str,
    operation: Operation,
) -> Result<Reply, Failure> {
    session.invoke_until(service, operation, deadline)
}
pub(crate) fn canonical(value: &Value) -> Result<String, Failure> {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(k, v)| (k.clone(), sorted(v)))
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
            ),
            Value::Array(values) => Value::Array(values.iter().map(sorted).collect()),
            _ => value.clone(),
        }
    }
    serde_json::to_string(&sorted(value)).map_err(|_| Failure::permanent("serialization_failed"))
}
