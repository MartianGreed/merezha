use crate::{
    Authorizer, Guest, GuestSession,
    config::Config,
    durable::{CommitCommand, DurableStore, ProjectionRef},
    instrument::{Dispatch, Instruments, Sample, SampledProjection, Settled},
    store::{EventStore, MemoryEventStore},
};
use merezha_domain::{
    Command, Failure, Identity, MessageKind, Operation, Query, Receipt, Reply, compatible_json,
};
use merezha_telemetry::names::dispatch::Trigger;
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
/// An idempotency key: `(tag, tenant, actor, key)`. Its JSON encoding is
/// the SQLite receipt key; without a tenant it is byte-identical to the
/// encoding before tenants existed.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Key {
    pub(crate) tag: Identity,
    /// The principal's tenant on tenant-scoped routes (AP-C2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tenant: Option<Identity>,
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
    /// Whether completed records stay in `idempotency`. Memory and SQLite
    /// keep them (SQLite preloads them at open, within `max_idempotency`).
    /// PostgreSQL has no such cap, so the map holds pending claims only and
    /// completed receipts come from the store.
    retain_receipts: bool,
    projections: BTreeMap<Identity, Mutex<ProjectionState>>,
    active: AtomicUsize,
    /// Spans, instruments and the lag cache (TE §1.3).
    instruments: Instruments,
    /// The typed PostgreSQL store behind `store`, for background runs,
    /// explicit catch-ups and the admin API.
    #[cfg(feature = "postgres")]
    postgres: Option<Arc<crate::postgres::PostgresStore>>,
}
/// A process-local `EventStore` port, or a durable store that also keeps
/// receipts and projection pairs (SQLite or PostgreSQL).
enum Storage {
    Volatile(Arc<dyn EventStore>),
    Durable(Arc<dyn DurableStore>),
}
impl Storage {
    fn load(
        &self,
        stream: &Identity,
        deadline: std::time::Instant,
    ) -> Result<Vec<merezha_domain::StoredEvent>, Failure> {
        match self {
            Self::Volatile(s) => s.load(stream),
            Self::Durable(s) => s.load(stream, deadline),
        }
    }
    fn head(&self, deadline: std::time::Instant) -> Result<u64, Failure> {
        match self {
            Self::Volatile(s) => s.head(),
            Self::Durable(s) => s.head(deadline),
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
            Self::Durable(s) => s.read_all(after, limit, deadline),
        }
    }
}
/// The bound of one lag sample (and of the head read at open).
const SAMPLE_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

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
        if !self.inner.retain_receipts {
            records.remove(&self.key);
        } else if let Some(record) = records.get_mut(&self.key) {
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
                Storage::Durable(Arc::new(store)),
                records,
                recovered,
                #[cfg(feature = "postgres")]
                None,
            )
        } else if let StorageConfig::Postgres { .. } = &config.storage {
            // PostgreSQL needs the host's pool and an async connect:
            // `PostgresStore::connect`, then `Application::with_postgres`.
            Err(Failure::permanent("storage_configuration"))
        } else {
            Self::in_memory(config, guest, authorizer)
        }
    }
    /// Construct over a connected PostgreSQL store. Synchronous: the store
    /// already checked the schema, the manifest and the projection rows.
    /// Every dispatch must then run on a blocking thread
    /// (`spawn_blocking`) or a thread outside the runtime, never on an
    /// async worker, because the store blocks on its runtime handle.
    #[cfg(feature = "postgres")]
    pub fn with_postgres(
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
        store: Arc<crate::postgres::PostgresStore>,
    ) -> Result<Self, Failure> {
        store.check_config(&config)?;
        Self::construct(
            config,
            guest,
            authorizer,
            Storage::Durable(store.clone()),
            BTreeMap::new(),
            BTreeMap::new(),
            Some(store),
        )
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
            #[cfg(feature = "postgres")]
            None,
        )
    }
    fn construct(
        config: Config,
        guest: Arc<dyn Guest>,
        authorizer: Arc<dyn Authorizer>,
        store: Storage,
        records: BTreeMap<Key, Record>,
        mut recovered: BTreeMap<Identity, ProjectionState>,
        #[cfg(feature = "postgres")] postgres: Option<Arc<crate::postgres::PostgresStore>>,
    ) -> Result<Self, Failure> {
        validate_services(&config, guest.as_ref())?;
        let retain_receipts = !matches!(
            config.storage,
            crate::config::StorageConfig::Postgres { .. }
        );
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
        // Memory and SQLite know their head and checkpoints at open; a
        // PostgreSQL application learns them from its first sample or run,
        // since other instances write too.
        let instruments = if !retain_receipts {
            Instruments::new(
                &config,
                None,
                config
                    .projections
                    .iter()
                    .map(|p| (p.name.as_str().to_owned(), None))
                    .collect(),
            )
        } else {
            let deadline = std::time::Instant::now() + SAMPLE_BUDGET;
            Instruments::new(
                &config,
                store.head(deadline).ok(),
                projections
                    .iter()
                    .map(|(name, state)| {
                        let checkpoint = state.lock().map(|s| s.checkpoint).ok();
                        (name.as_str().to_owned(), checkpoint)
                    })
                    .collect(),
            )
        };
        Ok(Self(Arc::new(Inner {
            config,
            guest,
            authorizer,
            store,
            projections,
            idempotency: Mutex::new(records),
            retain_receipts,
            active: AtomicUsize::new(0),
            instruments,
            #[cfg(feature = "postgres")]
            postgres,
        })))
    }
    /// Build the `merezha.command.*`, `merezha.query.*`, `merezha.events.*`
    /// and `merezha.projection.*` instruments once from `meter`, with the
    /// run-lock instruments of a PostgreSQL store. Call it before the first
    /// dispatch; later calls keep the first meter. Without it spans are
    /// still recorded and no instrument exists, as with disabled `meters`.
    pub fn with_meters(self, meters: &merezha_telemetry::Meters) -> Self {
        #[cfg(feature = "postgres")]
        let locks = self
            .0
            .postgres
            .as_ref()
            .map(|store| store.lock_source() as Arc<dyn crate::instrument::LockSource>);
        #[cfg(not(feature = "postgres"))]
        let locks = None;
        self.0.instruments.install(meters, locks);
        self
    }
    /// Refresh the lag gauges from the store: the head, every configured
    /// projection's checkpoint and the age of its first unprocessed event
    /// (and on PostgreSQL its warmth and rebuild progress). Blocking, like
    /// any dispatch, and bounded by 2 s. It takes no admission slot.
    pub fn sample_lag(&self) -> Result<(), Failure> {
        let sample = self.sample(std::time::Instant::now() + SAMPLE_BUDGET)?;
        self.0.instruments.apply(sample);
        Ok(())
    }
    /// Run [`Application::sample_lag`] every `interval` on blocking threads
    /// of the current runtime, until the application is dropped or the
    /// returned handle aborts it. A failed sample keeps the last values.
    /// Start it only when metrics are exported: each sample reads the store.
    pub fn spawn_lag_sampler(
        &self,
        interval: std::time::Duration,
    ) -> Result<tokio::task::AbortHandle, Failure> {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| Failure::permanent("storage_configuration"))?;
        let weak = Arc::downgrade(&self.0);
        let task = handle.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                let app = Application(inner);
                let _ = merezha_telemetry::spawn_blocking_in_span(move || app.sample_lag()).await;
            }
        });
        Ok(task.abort_handle())
    }
    #[cfg(feature = "postgres")]
    pub(crate) fn instruments(&self) -> &Instruments {
        &self.0.instruments
    }
    /// What [`Application::sample_lag`] reads.
    fn sample(&self, deadline: std::time::Instant) -> Result<Sample, Failure> {
        #[cfg(feature = "postgres")]
        if let Some(store) = &self.0.postgres {
            let projections: Vec<_> = self.0.config.projections.iter().collect();
            let (head, rows) = store.overview(&projections, deadline)?;
            return Ok(Sample {
                head,
                projections: rows
                    .into_iter()
                    .map(|row| SampledProjection {
                        name: row.entry.name.as_str().to_owned(),
                        checkpoint: row.entry.checkpoint,
                        oldest_ms: row.oldest.and_then(|ms| u64::try_from(ms).ok()),
                        warm: Some(row.entry.warm),
                        rebuild_remaining: row
                            .entry
                            .rebuild
                            .map(|r| r.target.saturating_sub(r.checkpoint)),
                    })
                    .collect(),
            });
        }
        let head = self.0.store.head(deadline)?;
        let mut projections = Vec::with_capacity(self.0.config.projections.len());
        for projection in &self.0.config.projections {
            let checkpoint = match &self.0.store {
                Storage::Durable(store) => {
                    store
                        .projection(&projection_ref(projection), deadline)?
                        .0
                        .checkpoint
                }
                // A running catch-up updates the cache itself.
                Storage::Volatile(_) => match self.0.projections[&projection.name].try_lock() {
                    Ok(state) => state.checkpoint,
                    Err(_) => continue,
                },
            };
            let oldest_ms = if checkpoint < head {
                self.0
                    .store
                    .read_all(checkpoint, 1, deadline)?
                    .first()
                    .map(|e| e.accepted_unix_ms)
            } else {
                None
            };
            projections.push(SampledProjection {
                name: projection.name.as_str().to_owned(),
                checkpoint,
                oldest_ms,
                warm: None,
                rebuild_remaining: None,
            });
        }
        Ok(Sample { head, projections })
    }
    #[cfg(feature = "postgres")]
    pub(crate) fn config(&self) -> &Config {
        &self.0.config
    }
    #[cfg(feature = "postgres")]
    pub(crate) fn guest(&self) -> &dyn Guest {
        self.0.guest.as_ref()
    }
    #[cfg(feature = "postgres")]
    pub(crate) fn postgres(&self) -> Option<Arc<crate::postgres::PostgresStore>> {
        self.0.postgres.clone()
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
    /// The status document (AP §D.1): head, per-projection checkpoint, lag
    /// and failure, and on PostgreSQL this instance's background runs and
    /// listener. Call it like any dispatch, from a blocking thread.
    pub fn status(&self) -> Result<crate::status::ApplicationStatus, Failure> {
        self.admit()?.status()
    }
}
#[cfg(feature = "test-support")]
impl Application {
    /// Test support for the store contract: a second application over the
    /// same durable data with its own connections, pending-claim registry
    /// and projection cache, as another host has. Memory storage has none.
    #[doc(hidden)]
    pub fn independent_instance(&self) -> Result<Self, Failure> {
        let Storage::Durable(store) = &self.0.store else {
            return Err(Failure::permanent("storage_configuration"));
        };
        let (store, (records, recovered)) = store.independent(&self.0.config)?;
        Self::construct(
            self.0.config.clone(),
            self.0.guest.clone(),
            self.0.authorizer.clone(),
            Storage::Durable(store),
            records,
            recovered,
            #[cfg(feature = "postgres")]
            None,
        )
    }
    /// A catch-up or rebuild on PostgreSQL that skips the run lock and
    /// computes on the caller's thread, publishing with row-revision CAS
    /// only: what a run whose lock session died does. For the lost-CAS
    /// contract tests.
    #[doc(hidden)]
    pub fn project_unlocked(&self, name: &str, rebuild: bool) -> Result<ProjectionState, Failure> {
        self.admit()?.project_inline(name, rebuild)
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
        trace: &Dispatch<'_>,
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
            Some(trace),
        )? {
            Reply::Validated { payload } => {
                self.check_value(&payload, self.inner.config.limits.max_payload_bytes)?;
                Ok(payload)
            }
            _ => Err(Failure::permanent("invalid_response")),
        }
    }
    fn check_value(&self, value: &Value, maximum: usize) -> Result<(), Failure> {
        check_value(value, maximum)
    }
    /// The in-process record for `key`: a completed receipt replays, a
    /// pending claim is `idempotency_in_flight`, another fingerprint is
    /// `idempotency_mismatch`. `None` when this instance has no record.
    fn local_record(
        &self,
        key: &Key,
        fingerprint: &str,
    ) -> Result<Option<Result<Receipt, Failure>>, Failure> {
        let records = self
            .inner
            .idempotency
            .lock()
            .map_err(|_| Failure::transient("internal"))?;
        Ok(records.get(key).map(|record| replay(record, fingerprint)))
    }
    /// Dispatch a command inside its `command {tag}` span, recording its
    /// outcome, duration and appended events (TE §1.2, §1.3).
    pub fn command(self, command: Command) -> Result<Receipt, Failure> {
        let inner = self.inner.clone();
        let trace = inner
            .instruments
            .command(command.tag.as_str(), &command.metadata);
        let span = trace.span().clone();
        let result = span.in_scope(|| self.decide(command, &trace));
        trace.finish(match &result {
            Ok((receipt, true)) => Ok(Settled::Replayed {
                position: receipt.position,
            }),
            Ok((receipt, false)) => Ok(Settled::Committed {
                position: receipt.position,
                events: receipt.events,
            }),
            Err(failure) => Err(failure),
        });
        result.map(|(receipt, _)| receipt)
    }
    /// The command flow; `true` with a stored receipt replayed.
    fn decide(self, command: Command, trace: &Dispatch<'_>) -> Result<(Receipt, bool), Failure> {
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
        trace.stream(stream.as_str(), command.expected_version);
        let session = self.inner.guest.session()?;
        let payload = self.validate(
            session.as_ref(),
            MessageKind::Command,
            route.service.as_str(),
            &command.tag,
            command.payload,
            trace,
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
                tenant: None,
                actor: command.metadata.actor.clone(),
                key,
            };
            // Bind stream and explicit expected version too, so a key cannot
            // silently replay the result of another target or precondition.
            let fingerprint = canonical(
                &serde_json::json!({"stream":stream,"payload":payload,"expected_version":command.expected_version}),
            )?;
            if let Some(replay) = self.local_record(&key, &fingerprint)? {
                return replay.map(|receipt| (receipt, true));
            }
            // Fast-path replay of a key another instance (or an earlier
            // process) committed, before the guest decides. It runs before
            // the local capacity check, so a full local registry still
            // replays stored receipts. No map lock is held during SQL.
            if let Storage::Durable(store) = &self.inner.store
                && let Some(receipt) = store.receipt(&key, &fingerprint, self.deadline)?
            {
                return Ok((receipt, true));
            }
            let mut records = self
                .inner
                .idempotency
                .lock()
                .map_err(|_| Failure::transient("internal"))?;
            // A same-key caller on this instance may have claimed meanwhile.
            if let Some(record) = records.get(&key) {
                return replay(record, &fingerprint).map(|receipt| (receipt, true));
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
        let limits = &self.inner.config.limits;
        if history.len() > limits.max_events.min(limits.max_stream_events) {
            return Err(Failure::permanent("history_limit"));
        }
        let expected = history.last().map_or(0, |event| event.version);
        if command.expected_version.is_some_and(|v| v != expected) {
            // Another instance may have committed this very key since the
            // fast path, which is what made the precondition stale: replay
            // its receipt rather than refuse the retry.
            if let (Some(pending), Storage::Durable(store)) = (&claim, &self.inner.store)
                && let Some(receipt) =
                    store.receipt(&pending.key, &pending.fingerprint, self.deadline)?
            {
                return Ok((receipt, true));
            }
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
            Some(trace),
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
        let (receipt, replayed) = match &self.inner.store {
            Storage::Durable(store) => store.commit(
                CommitCommand {
                    stream: &stream,
                    tenant: None,
                    expected,
                    events,
                    metadata: &command.metadata,
                    claim: claim.as_ref().map(|c| (&c.key, c.fingerprint.as_str())),
                },
                self.deadline,
            )?,
            Storage::Volatile(store) => (
                store.append(&stream, expected, events, &command.metadata)?,
                false,
            ),
        };
        if let Some(claim) = claim {
            claim.complete(&receipt);
        }
        Ok((receipt, replayed))
    }
    /// Dispatch a query inside its `query {tag}` span (TE §1.2, §1.3).
    pub fn query(self, query: Query) -> Result<QueryResult, Failure> {
        let inner = self.inner.clone();
        let trace = inner.instruments.query(query.tag.as_str(), &query.metadata);
        let span = trace.span().clone();
        let result = span.in_scope(|| self.answer(query, &trace));
        trace.finish(match &result {
            Ok(answer) => Ok(Settled::Answered {
                checkpoint: answer.checkpoint,
            }),
            Err(failure) => Err(failure),
        });
        result
    }
    fn answer(self, query: Query, trace: &Dispatch<'_>) -> Result<QueryResult, Failure> {
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
            trace,
        )?;
        self.inner.authorizer.authorize(
            MessageKind::Query,
            query.tag.as_str(),
            None,
            &payload,
            &query.metadata,
        )?;
        let snapshot = match &self.inner.store {
            #[cfg(feature = "postgres")]
            Storage::Durable(_) if self.inner.postgres.is_some() => {
                let projection = self.configured(route.projection.as_str())?;
                self.served(projection, query.min_position)?
            }
            Storage::Durable(store) => {
                let projection = self.configured(route.projection.as_str())?;
                store
                    .projection(&projection_ref(projection), self.deadline)?
                    .0
            }
            Storage::Volatile(_) => self.inner.projections[&route.projection]
                .try_lock()
                .map_err(|_| Failure::conflict("projection_in_flight"))?
                .clone(),
        };
        // Without background runs nothing advances while waiting.
        if query.min_position.is_some_and(|p| snapshot.checkpoint < p) {
            return Err(Failure::transient("projection_behind"));
        }
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
            Some(trace),
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
    fn configured(&self, name: &str) -> Result<&crate::config::Projection, Failure> {
        self.inner
            .config
            .projections
            .iter()
            .find(|p| p.name.as_str() == name)
            .ok_or_else(|| Failure::permanent("projection_not_found"))
    }
    /// Catch up (`rebuild = false`) or rebuild a projection within the
    /// admitted deadline. Memory and SQLite run it here and fail
    /// `projection_in_flight` beside another run on this instance. On
    /// PostgreSQL a catch-up runs to the head snapshot under the run lock,
    /// or waits for the runner holding it; a rebuild is requested (or
    /// resumed) and run to its swap the same way, and its progress
    /// persists past `timeout`.
    pub fn project(self, name: &str, rebuild: bool) -> Result<ProjectionState, Failure> {
        #[cfg(feature = "postgres")]
        if let Some(store) = &self.inner.postgres {
            let projection = self.configured(name)?;
            let app = Application(self.inner.clone());
            return if rebuild {
                store.rebuild_now(&app, projection, self.deadline)
            } else {
                store.catch_up(&app, projection, self.deadline)
            };
        }
        self.project_inline(name, rebuild)
    }
    /// The run on the caller's thread, with row-revision CAS on durable
    /// stores and no run lock.
    fn project_inline(self, name: &str, rebuild: bool) -> Result<ProjectionState, Failure> {
        let projection = self.configured(name)?;
        let trigger = if rebuild {
            Trigger::Rebuild
        } else {
            Trigger::Admin
        };
        let mut run = self
            .inner
            .instruments
            .catch_up(projection.name.as_str(), trigger);
        let result = self.project_batches(projection, rebuild, &mut run);
        run.finish(result.as_ref().map(|_| ()));
        result.map_err(Failure::from)
    }
    fn project_batches(
        &self,
        projection: &crate::config::Projection,
        rebuild: bool,
        run: &mut crate::instrument::ProjectionRun<'_>,
    ) -> Result<ProjectionState, crate::instrument::RunFailure> {
        let session = self.inner.guest.session()?;
        let mut current = self.inner.projections[&projection.name]
            .try_lock()
            .map_err(|_| Failure::conflict("projection_in_flight"))?;
        let mut revision = 0;
        let target = projection_ref(projection);
        if let Storage::Durable(store) = &self.inner.store {
            let (snapshot, stored_revision) = store.projection(&target, self.deadline)?;
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
                return Err(Failure::permanent("invalid_event_feed").into());
            }
            let mut checkpoint = pending.checkpoint;
            for event in &events {
                if event.position != checkpoint + 1 {
                    return Err(Failure::permanent("invalid_event_feed").into());
                }
                checkpoint = event.position;
            }
            let count = events.len();
            // Memory and SQLite store no trace context, so no links.
            run.batch(
                events.first().map(|e| (e.position, e.accepted_unix_ms)),
                &[],
            );
            let span = run.span().clone();
            let reply = span
                .in_scope(|| {
                    invoke(
                        session.as_ref(),
                        self.deadline,
                        projection.service.as_str(),
                        Operation::Project {
                            projection: projection.name.clone(),
                            state: pending.state.clone(),
                            events,
                            live: !rebuild,
                        },
                        None,
                    )
                })
                .map_err(crate::instrument::RunFailure::from_guest_call)?;
            let Reply::Projected { state } = reply else {
                return Err(Failure::permanent("invalid_response").into());
            };
            self.check_value(&state, self.inner.config.limits.max_state_bytes)?;
            pending = ProjectionState { state, checkpoint };
            if !rebuild {
                if let Storage::Durable(store) = &self.inner.store {
                    revision = span.in_scope(|| {
                        store.commit_projection(&target, revision, &pending, self.deadline)
                    })?;
                }
                *current = pending.clone();
                run.published(count, Some(checkpoint));
            } else {
                run.published(count, None);
            }
        }
        // Rebuild swaps its entire candidate only after every batch succeeds.
        if rebuild && let Storage::Durable(store) = &self.inner.store {
            store.commit_projection(&target, revision, &pending, self.deadline)?;
        }
        if rebuild {
            run.published(0, Some(pending.checkpoint));
        }
        *current = pending;
        Ok(current.clone())
    }
}
impl Admission {
    /// The PostgreSQL served pair, refused while the revision warms
    /// (`projection_warming`). With `min_position` it waits, woken by local
    /// commits and polling every 50 ms, until the checkpoint reaches it,
    /// else fails `projection_behind` at the deadline.
    #[cfg(feature = "postgres")]
    fn served(
        &self,
        projection: &crate::config::Projection,
        min_position: Option<u64>,
    ) -> Result<ProjectionState, Failure> {
        let store = self
            .inner
            .postgres
            .as_ref()
            .ok_or_else(|| Failure::permanent("storage_configuration"))?;
        let mut published = store.shared().published.subscribe();
        loop {
            let read = store.served(projection, self.deadline);
            let (pair, _, warm) = match min_position {
                Some(_) => crate::postgres::at_deadline(read, self.deadline, || {
                    Failure::transient("projection_behind")
                })?,
                None => read?,
            };
            if pair.checkpoint < warm {
                return Err(Failure::transient("projection_warming"));
            }
            if min_position.is_none_or(|position| pair.checkpoint >= position) {
                return Ok(pair);
            }
            store.wait_published(&mut published, self.deadline, || {
                Failure::transient("projection_behind")
            })?;
        }
    }
    /// See [`Application::status`].
    pub fn status(self) -> Result<crate::status::ApplicationStatus, Failure> {
        use crate::status::{ApplicationStatus, ProjectionStatus};
        #[cfg(feature = "postgres")]
        if let Some(store) = &self.inner.postgres {
            let projections: Vec<_> = self.inner.config.projections.iter().collect();
            return store.status(&projections, self.deadline);
        }
        let head = self.inner.store.head(self.deadline)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut projections = Vec::with_capacity(self.inner.config.projections.len());
        for projection in &self.inner.config.projections {
            let checkpoint = match &self.inner.store {
                Storage::Durable(store) => {
                    store
                        .projection(&projection_ref(projection), self.deadline)?
                        .0
                        .checkpoint
                }
                Storage::Volatile(_) => {
                    self.inner.projections[&projection.name]
                        .lock()
                        .map_err(|_| Failure::transient("internal"))?
                        .checkpoint
                }
            };
            let lag_events = head.saturating_sub(checkpoint);
            let lag_seconds = if lag_events > 0 {
                self.inner
                    .store
                    .read_all(checkpoint, 1, self.deadline)?
                    .first()
                    .map_or(0.0, |e| {
                        now.saturating_sub(e.accepted_unix_ms) as f64 / 1000.0
                    })
            } else {
                0.0
            };
            projections.push(ProjectionStatus {
                name: projection.name.clone(),
                checkpoint,
                lag_events,
                lag_seconds,
                status: "active".into(),
                failure_code: None,
            });
        }
        Ok(ApplicationStatus {
            application: match &self.inner.config.storage {
                crate::config::StorageConfig::Memory => None,
                crate::config::StorageConfig::Sqlite { application, .. }
                | crate::config::StorageConfig::Postgres { application } => {
                    Some(application.clone())
                }
            },
            head,
            projections,
            reactors: Vec::new(),
            instance: None,
            listener: None,
        })
    }
}
/// The PostgreSQL admin operations (AP §D.1); other backends fail
/// `storage_configuration`.
#[cfg(feature = "postgres")]
impl Admission {
    fn pg(&self) -> Result<&crate::postgres::PostgresStore, Failure> {
        self.inner
            .postgres
            .as_deref()
            .ok_or_else(|| Failure::permanent("storage_configuration"))
    }
    /// Every configured revision with its lag, warmth, failure and rebuild.
    pub fn projections(self) -> Result<crate::status::ProjectionsOverview, Failure> {
        let projections: Vec<_> = self.inner.config.projections.iter().collect();
        let (head, rows) = self.pg()?.overview(&projections, self.deadline)?;
        Ok(crate::status::ProjectionsOverview {
            head,
            projections: rows.into_iter().map(|row| row.entry).collect(),
        })
    }
    /// One configured projection, plus every revision row of its name.
    pub fn projection(self, name: &str) -> Result<crate::status::ProjectionEntry, Failure> {
        let projection = self.configured(name)?;
        let store = self.pg()?;
        let (_, rows) = store.overview(&[projection], self.deadline)?;
        let mut entry = rows
            .into_iter()
            .next()
            .ok_or_else(|| Failure::permanent("projection_not_found"))?
            .entry;
        entry.revisions = Some(store.revisions(&projection.name, self.deadline)?);
        Ok(entry)
    }
    /// `POST .../rebuild`: start (or with `resume`, resume) a background
    /// rebuild of the configured revision up to the current head.
    pub fn request_rebuild(
        self,
        name: &str,
        resume: bool,
        requested_by: Option<&str>,
    ) -> Result<crate::status::RebuildRequested, Failure> {
        let projection = self.configured(name)?;
        let target = self
            .pg()?
            .request_rebuild(projection, resume, requested_by, self.deadline)?;
        Ok(crate::status::RebuildRequested {
            name: projection.name.clone(),
            revision: projection.revision,
            target,
        })
    }
    /// `DELETE .../rebuild`: abandon a failed or running rebuild.
    pub fn abandon_rebuild(self, name: &str) -> Result<(), Failure> {
        let projection = self.configured(name)?;
        self.pg()?.abandon_rebuild(projection, self.deadline)
    }
    /// `DELETE .../revisions/{revision}`: drop a revision row this host does
    /// not serve; `revision_active` for the configured one.
    pub fn delete_revision(self, name: &str, revision: u32) -> Result<(), Failure> {
        let name = Identity::new(name).map_err(|_| Failure::permanent("projection_not_found"))?;
        self.pg()?.delete_revision(&name, revision, self.deadline)
    }
}
/// `compatible_json` and at most `maximum` encoded bytes, else
/// `payload_limit`.
pub(crate) fn check_value(value: &Value, maximum: usize) -> Result<(), Failure> {
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
/// The answer an in-process record gives: its completed receipt, or
/// `idempotency_in_flight` while pending, or `idempotency_mismatch` for
/// another fingerprint.
fn replay(record: &Record, fingerprint: &str) -> Result<Receipt, Failure> {
    if record.payload != fingerprint {
        return Err(Failure::conflict("idempotency_mismatch"));
    }
    record
        .result
        .clone()
        .ok_or_else(|| Failure::conflict("idempotency_in_flight"))
}
fn projection_ref(projection: &crate::config::Projection) -> ProjectionRef<'_> {
    ProjectionRef {
        name: &projection.name,
        revision: projection.revision,
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
    trace: Option<&Dispatch<'_>>,
) -> Result<Reply, Failure> {
    let result = session.invoke_until(service, operation, deadline);
    if let (Err(failure), Some(trace)) = (&result, trace) {
        trace.guest_failed(failure);
    }
    result
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
