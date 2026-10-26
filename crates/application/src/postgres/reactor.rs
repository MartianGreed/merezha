//! Reactors on PostgreSQL (AP §C.5, R4): the `react` runner under the `r:`
//! advisory lock, the two-tier reply check, the checkpoint transaction that
//! writes every accepted effect through the host's [`Effects`] port, and
//! the failure semantics (backoff, isolation of a poison event, halt and
//! `on_error = "skip"`).
//!
//! The application never depends on the platform: [`Effects`] is the seam,
//! implemented by the platform host over its `Notifier`, `Outbox` and
//! `Directory` ports. A batch runs in this order, so the checkpoint
//! transaction only writes and no port call inside it waits on another
//! pooled connection:
//!
//! 1. read up to `batch_size` events after the checkpoint and keep those
//!    whose tag the reactor's `events` filter names (none: advance only);
//! 2. call the guest `react` outside SQL and check its reply: a malformed
//!    reply is the reactor's failure, a well-formed effect that fails
//!    validation is rejected and recorded;
//! 3. run the port's pure [`Effects::check`] on each notify effect and ask
//!    [`Effects::is_member`] for the tenant membership of its user
//!    recipients;
//! 4. one transaction on the run's detached session: lock the row (CAS on
//!    `row_revision` and the checkpoint, not paused), [`Effects::write`]
//!    each accepted effect, record the rejections, advance.
use super::{
    PostgresStore, invalid, read_error,
    runner::{Bounds, LINGER, pause_in},
    signed, sql, unsigned,
};
use crate::{
    Application,
    config::{EffectKind, OnError, Reactor},
    instrument::RunFailure,
};
use merezha_domain::{
    Channel, Classification, Effect, Failure, Identity, MAX_EFFECT_DELAY_MS, MAX_EFFECTS_PER_BATCH,
    MAX_EFFECTS_PER_EVENT, Operation, Reaction, Recipient, Reply, StoredEvent, compatible_json,
};
use merezha_postgres::{BoxFuture, DetachedClient, Tx};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;

/// After this many consecutive transient guest failures of a batch of
/// several events, the batch is retried one event at a time.
pub const ISOLATE_AFTER: u32 = 3;
/// The queue command effects run on (`[jobs.queues.application]`).
pub const COMMAND_QUEUE: &str = "application";
/// The job kind of a command effect, handled by the host's
/// `application.command` handler.
pub const COMMAND_KIND: &str = "application.command";
/// The outbox and notifier source of every effect (R4).
pub const SOURCE: &str = "application";

/// One accepted effect as the host writes it (AP §C.5), built from the
/// guest's [`Effect`] and its event. Guests never set the dedupe key, the
/// tenant or the source event.
#[derive(Clone, Debug, PartialEq)]
pub struct EffectRequest {
    pub reactor: Identity,
    /// The event's position and the effect's zero-based index in its list.
    pub position: u64,
    pub index: u8,
    /// `reaction:{reactor}:{event_id}:{index}`, at most 160 bytes.
    pub dedupe_key: String,
    /// The event's stream tenant; `None` for an application stream.
    pub tenant: Option<Identity>,
    /// The event's correlation id.
    pub correlation_id: Identity,
    /// The event's id, `event/{position}`: the causation of the effect.
    pub event_id: Identity,
    pub accepted_unix_ms: u64,
    pub body: EffectBody,
}
/// What an [`EffectRequest`] does.
#[derive(Clone, Debug, PartialEq)]
pub enum EffectBody {
    /// `Notifier::request` with source `application`; `not_before` is now
    /// plus `delay_ms`.
    Notify {
        topic: Identity,
        recipients: Vec<Recipient>,
        /// `None`: the topic's default channels.
        channels: Option<Vec<Channel>>,
        data: Map<String, Value>,
        delay_ms: Option<u64>,
        /// The `user` recipients confirmed members of the event's tenant
        /// through [`Effects::is_member`]; empty without a tenant.
        members: Vec<Identity>,
    },
    /// `Notifier::emit_webhook`: the event's tenant endpoints, or the
    /// application-level ones without a tenant; `source_event_id` is the
    /// dedupe key and `occurred_at` the event's accepted time.
    Webhook { event_type: Identity, data: Value },
    /// An `Outbox::append` of one job `application`/`application.command`
    /// keyed by the dedupe key, running at now plus `delay_ms`.
    Command {
        job: CommandJob,
        delay_ms: Option<u64>,
    },
}
/// The payload of an `application.command` job: what the host's handler
/// needs to run the command as the reactor's system principal. `Debug`
/// never prints the command payload.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandJob {
    pub reactor: Identity,
    pub position: u64,
    pub index: u8,
    pub event_id: Identity,
    pub correlation_id: Identity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<Identity>,
    pub tag: Identity,
    pub stream: Identity,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u64>,
}
impl std::fmt::Debug for CommandJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandJob")
            .field("reactor", &self.reactor)
            .field("position", &self.position)
            .field("index", &self.index)
            .field("tag", &self.tag)
            .finish_non_exhaustive()
    }
}
impl CommandJob {
    /// The command's idempotency key and message id,
    /// `rx:{reactor}:{position}:{index}`: a duplicate delivery of the job
    /// replays the stored receipt.
    pub fn idempotency_key(&self) -> Result<Identity, Failure> {
        Identity::new(format!(
            "rx:{}:{}:{}",
            self.reactor.as_str(),
            self.position,
            self.index
        ))
    }
}

/// The host's effect ports, seen from the application (AP §C.5): the
/// platform implements it over `Notifier`, `Outbox` and `Directory`, bound
/// to the source `application`.
///
/// Contract: [`Effects::check`] is pure. [`Effects::is_member`] runs on its
/// own pooled connection and is only called before the checkpoint
/// transaction opens. [`Effects::write`] writes inside the caller's
/// transaction and never commits it; it fails *permanent* only for an
/// effect it refused before writing anything (the effect is then recorded
/// as rejected and the batch goes on), and with any other classification
/// when the write failed (the transaction rolls back and the batch
/// retries).
pub trait Effects: Send + Sync {
    /// The notifications block's pure `validate` for a notify effect; `Ok`
    /// for other effects. A failure rejects the effect with its code.
    fn check(&self, request: &EffectRequest) -> Result<(), Failure>;
    /// `Directory::is_member(user, tenant)`.
    fn is_member<'a>(
        &'a self,
        user: &'a Identity,
        tenant: &'a Identity,
    ) -> BoxFuture<'a, Result<bool, Failure>>;
    /// The effect's write inside the checkpoint transaction.
    fn write<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        request: &'a EffectRequest,
    ) -> BoxFuture<'a, Result<(), Failure>>;
}

/// Per configured reactor, shared by the scheduler, the runs and the
/// admin API on this instance.
#[derive(Default)]
pub(crate) struct ReactorLane {
    /// Wakes the lane; signals coalesce.
    pub wake: Notify,
    /// Since when this instance holds the `r:` lock.
    held: Mutex<Option<Instant>>,
    /// Lock attempts that found another session holding it.
    pub contended: AtomicU64,
    /// While the checkpoint is below this position, batches read one event
    /// at a time: a permanent failure on a larger batch is being isolated.
    narrow: AtomicU64,
    /// Whether the reactor was halted when this instance last looked.
    pub halted: AtomicBool,
}
/// Clears the lock-held time when the run ends, however it ends.
struct Held<'a>(&'a Mutex<Option<Instant>>);
impl Drop for Held<'_> {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}
impl ReactorLane {
    /// Seconds this instance has held the lock, when it holds it.
    pub(crate) fn held_for(&self) -> Option<f64> {
        self.held
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map(|since| since.elapsed().as_secs_f64())
    }
}

/// What a reactor run did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum ReactorOutcome {
    /// Another session holds the `r:` lock; nothing ran.
    Contended,
    /// Caught up, or stopped at the run's soft deadline (`behind`).
    Finished { behind: bool },
    /// Paused (`None`), or waiting for its retry time.
    Waiting(Option<Duration>),
}

/// The reactor row as a run reads it.
struct Row {
    checkpoint: u64,
    row_revision: i64,
    status: String,
    failures: u32,
    /// Milliseconds until `retry_at`; 0 when due.
    retry_ms: u64,
}
/// One rejected effect, recorded in `reactor_rejections`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Rejection {
    pub position: u64,
    pub index: u8,
    pub effect: &'static str,
    pub code: String,
}
/// What one batch amounts to before its transaction.
struct Plan {
    accepted: Vec<EffectRequest>,
    rejected: Vec<Rejection>,
}

/// One reactor run's locked session.
struct Session<'a> {
    store: &'a PostgresStore,
    bounds: &'a Bounds,
    client: DetachedClient,
    name: String,
    _held: Held<'a>,
}

/// Feed rows: events, stored trace contexts and stream tenants.
type Feed = (
    Vec<StoredEvent>,
    Vec<Option<merezha_telemetry::propagation::StoredContext>>,
    Vec<Option<String>>,
);

/// Whether a reactor lane should take its lock now.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Due {
    Run,
    /// Caught up.
    Idle,
    Paused,
    /// Its backoff ends after this.
    Later(Duration),
}

impl PostgresStore {
    /// The lane's cheap pooled read: the row and the head.
    pub(crate) async fn reactor_due(&self, name: &str, deadline: Instant) -> Result<Due, Failure> {
        let (row, head) = self
            .session(deadline, super::timeout, async |client| {
                let row = self
                    .db
                    .query_opt(client, &sql::REACTOR, &[&name])
                    .await
                    .map_err(|e| read_error(&e))?;
                let head: i64 = self
                    .db
                    .query_one(client, &sql::HEAD, &[])
                    .await
                    .map_err(|e| read_error(&e))?
                    .get(0);
                Ok((row, head))
            })
            .await?;
        let row = row.ok_or_else(invalid)?;
        let (checkpoint, status, retry_ms): (i64, String, i64) =
            (row.get(0), row.get(2), row.get(5));
        Ok(if status == "paused" {
            Due::Paused
        } else if retry_ms > 0 {
            Due::Later(Duration::from_millis(unsigned(retry_ms)?))
        } else if checkpoint >= head {
            Due::Idle
        } else {
            Due::Run
        })
    }
    pub(crate) fn reactor_lane(&self, name: &str) -> Result<&ReactorLane, Failure> {
        self.shared()
            .reactors
            .get(name)
            .ok_or_else(|| Failure::permanent("reactor_not_found"))
    }
    /// Detach a session, name it `merezha r:{name}` and try the lock.
    fn lock_reactor<'a>(
        &'a self,
        bounds: &'a Bounds,
        reactor: &Reactor,
    ) -> Result<Option<Session<'a>>, Failure> {
        let lane = self.reactor_lane(reactor.name.as_str())?;
        let name = reactor.name.as_str().to_owned();
        let key = format!("r:{name}");
        let client = self.step(bounds, self.db.detach(bounds.hard))?;
        let label = format!("merezha {key}");
        self.step(bounds, async {
            self.db
                .query_one(&client, &sql::NAME_SESSION, &[&label])
                .await
                .map_err(|e| read_error(&e))
        })?;
        if !self.shared().options.hooks().skip_lock {
            let locked: bool = self
                .step(bounds, async {
                    self.db
                        .query_one(&client, &sql::TRY_LOCK, &[&key])
                        .await
                        .map_err(|e| read_error(&e))
                })?
                .get(0);
            if !locked {
                lane.contended.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
        }
        *lane.held.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
        Ok(Some(Session {
            store: self,
            bounds,
            client,
            name,
            _held: Held(&lane.held),
        }))
    }
}

impl Session<'_> {
    fn row(&self) -> Result<Row, Failure> {
        let row = self.store.step(self.bounds, async {
            self.store
                .db
                .query_opt(&self.client, &sql::REACTOR, &[&self.name])
                .await
                .map_err(|e| read_error(&e))
        })?;
        let row = row.ok_or_else(invalid)?;
        Ok(Row {
            checkpoint: unsigned(row.get(0))?,
            row_revision: row.get(1),
            status: row.get(2),
            failures: u32::try_from(row.get::<_, i32>(4)).map_err(|_| invalid())?,
            retry_ms: unsigned(row.get(5))?,
        })
    }
    fn head(&self) -> Result<u64, Failure> {
        let row = self.store.step(self.bounds, async {
            self.store
                .db
                .query_one(&self.client, &sql::HEAD, &[])
                .await
                .map_err(|e| read_error(&e))
        })?;
        unsigned(row.get(0))
    }
    /// At most `limit` contiguous events after `after`.
    fn feed(&self, after: u64, limit: u64) -> Result<Feed, Failure> {
        let (from, count) = (signed(after)?, signed(limit)?);
        let rows = self.store.step(self.bounds, async {
            self.store
                .db
                .query(&self.client, &sql::READ_ALL, &[&from, &count])
                .await
                .map_err(|e| read_error(&e))
        })?;
        let events = self.store.events(&rows)?;
        if events.is_empty()
            || events
                .iter()
                .zip(after + 1..)
                .any(|(event, expected)| event.position != expected)
        {
            return Err(Failure::transient("event_feed_gap"));
        }
        let tenants = rows.iter().map(|row| row.get(10)).collect();
        Ok((events, PostgresStore::traces(&rows), tenants))
    }
    /// The checkpoint transaction (AP §C.5): lock the row and compare it,
    /// write each accepted effect, record the rejections (with those the
    /// port refused) and `skip`, and advance to `end`. Returns the types
    /// of the effects written and the rejections the port added. `reactor_conflict` when the row moved
    /// or was paused; nothing is written then.
    fn commit(
        &mut self,
        expected: &Row,
        end: u64,
        plan: Plan,
        skip: Option<(u64, String)>,
        effects: Arc<dyn Effects>,
    ) -> Result<(Vec<&'static str>, Vec<Rejection>), Failure> {
        let name = self.name.clone();
        let (start, revision) = (signed(expected.checkpoint)?, expected.row_revision);
        let end = signed(end)?;
        let hooks = self.store.shared().options.hooks();
        let pause = hooks.pause.clone();
        let hard = self.bounds.hard;
        let (store, bounds) = (self.store, self.bounds);
        let db = store.db.clone();
        store
            .step(
                bounds,
                self.client.transaction(hard, move |tx| {
                    Box::pin(async move {
                        let row = db
                            .query_opt(tx, &sql::LOCK_REACTOR, &[&name])
                            .await
                            .map_err(|e| read_error(&e))?
                            .ok_or_else(invalid)?;
                        let (checkpoint, row_revision, status): (i64, i64, String) =
                            (row.get(0), row.get(1), row.get(2));
                        if checkpoint != start || row_revision != revision || status == "paused" {
                            return Err(Failure::conflict("reactor_conflict"));
                        }
                        let Plan {
                            accepted,
                            mut rejected,
                        } = plan;
                        let before = rejected.len();
                        let mut written = Vec::with_capacity(accepted.len());
                        for request in &accepted {
                            match effects.write(tx, request).await {
                                Ok(()) => written.push(request.body.kind()),
                                Err(failure)
                                    if failure.classification == Classification::Permanent =>
                                {
                                    rejected.push(Rejection {
                                        position: request.position,
                                        index: request.index,
                                        effect: request.body.kind(),
                                        code: failure.code,
                                    })
                                }
                                Err(failure) => return Err(failure),
                            }
                        }
                        if !rejected.is_empty() {
                            let positions = rejected
                                .iter()
                                .map(|r| signed(r.position))
                                .collect::<Result<Vec<i64>, _>>()?;
                            let indexes: Vec<i16> =
                                rejected.iter().map(|r| i16::from(r.index)).collect();
                            let kinds: Vec<&str> = rejected.iter().map(|r| r.effect).collect();
                            let codes: Vec<&str> =
                                rejected.iter().map(|r| r.code.as_str()).collect();
                            db.execute(
                                tx,
                                &sql::INSERT_REJECTIONS,
                                &[&name, &positions, &indexes, &kinds, &codes],
                            )
                            .await
                            .map_err(|e| read_error(&e))?;
                        }
                        if let Some((position, code)) = &skip {
                            let position = signed(*position)?;
                            db.execute(
                                tx,
                                &sql::INSERT_SKIP,
                                &[
                                    &name,
                                    &position,
                                    &"on_error_skip",
                                    code,
                                    &None::<String>,
                                    &position,
                                ],
                            )
                            .await
                            .map_err(|e| read_error(&e))?;
                        }
                        db.query_one(tx, &sql::ADVANCE_REACTOR, &[&name, &end])
                            .await
                            .map_err(|e| read_error(&e))?;
                        pause_in(&db, tx, pause, &name).await?;
                        forced_rollback(&hooks, &name)?;
                        #[cfg(test)]
                        super::tests::crash_cut("reactor_before_commit");
                        Ok((written, rejected.split_off(before)))
                    })
                }),
            )
            .inspect(|_| {
                #[cfg(test)]
                super::tests::crash_cut("reactor_after_commit");
            })
    }
    /// Failure bookkeeping of a batch that did not commit: `halt` at a
    /// position, or only a later retry. The checkpoint stays.
    fn record_failure(
        &self,
        failure: &Failure,
        checkpoint: u64,
        delay: Duration,
        halt: Option<u64>,
    ) -> Result<(), Failure> {
        let class = classification(failure.classification);
        let delay = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
        let halt = halt.map(signed).transpose()?;
        let checkpoint = signed(checkpoint)?;
        self.store.step(self.bounds, async {
            self.store
                .db
                .query_opt(
                    &self.client,
                    &sql::REACTOR_FAILURE,
                    &[
                        &self.name,
                        &failure.code,
                        &class,
                        &delay,
                        &halt,
                        &checkpoint,
                    ],
                )
                .await
                .map_err(|e| read_error(&e))
        })?;
        Ok(())
    }
}

/// The test hook `RunHooks::reactor_fail` (feature `test-support`).
#[cfg(feature = "test-support")]
fn forced_rollback(hooks: &super::RunHooks, reactor: &str) -> Result<(), Failure> {
    match &hooks.reactor_fail {
        Some(fail) => fail.check(reactor),
        None => Ok(()),
    }
}
#[cfg(not(feature = "test-support"))]
fn forced_rollback(_: &super::RunHooks, _: &str) -> Result<(), Failure> {
    Ok(())
}

fn classification(class: Classification) -> &'static str {
    match class {
        Classification::Permanent => "permanent",
        Classification::Transient => "transient",
        Classification::Conflict => "conflict",
    }
}

impl EffectBody {
    /// `notify`, `webhook` or `command`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Notify { .. } => "notify",
            Self::Webhook { .. } => "webhook",
            Self::Command { .. } => "command",
        }
    }
}

/// The equal-jitter backoff of the `failures`-th consecutive failure:
/// half of `min(base * 2^(failures - 1), max)` plus a random share of the
/// other half.
pub(crate) fn backoff(base: Duration, max: Duration, failures: u32) -> Duration {
    let ceiling = base
        .saturating_mul(1u32 << failures.saturating_sub(1).min(20))
        .min(max);
    let half = ceiling / 2;
    let spread = u64::try_from(half.as_millis()).unwrap_or(u64::MAX);
    half + Duration::from_millis(if spread == 0 {
        0
    } else {
        jitter() % (spread + 1)
    })
}
/// Not a secret: per-call randomness from the standard library's hasher
/// keys.
fn jitter() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    hasher.finish()
}

/// The malformed tier (AP §C.2): the reply must be `reacted` with exactly
/// one reaction per input event, in order, each naming its event, with at
/// most 8 effects per event and 64 per batch, every effect within
/// `max_payload_bytes` and JSON-compatible. Anything else is
/// `invalid_response`.
fn reactions(
    reply: Reply,
    events: &[StoredEvent],
    max_payload_bytes: usize,
) -> Result<Vec<Reaction>, Failure> {
    let malformed = || Failure::permanent("invalid_response");
    let Reply::Reacted { reactions } = reply else {
        return Err(malformed());
    };
    if reactions.len() != events.len()
        || reactions
            .iter()
            .zip(events)
            .any(|(reaction, event)| reaction.event_id != event.event_id)
        || reactions
            .iter()
            .any(|r| r.effects.len() > MAX_EFFECTS_PER_EVENT)
        || reactions.iter().map(|r| r.effects.len()).sum::<usize>() > MAX_EFFECTS_PER_BATCH
    {
        return Err(malformed());
    }
    for effect in reactions.iter().flat_map(|r| &r.effects) {
        let value = serde_json::to_value(effect).map_err(|_| malformed())?;
        compatible_json(&value).map_err(|_| malformed())?;
        if value.to_string().len() > max_payload_bytes {
            return Err(malformed());
        }
    }
    Ok(reactions)
}

/// The rejected tier on the host's side (AP §C.2), before any port: the
/// effect type the reactor allows, its command tags, address recipients
/// only with `allow_addresses`, a `tenant` recipient only for the event's
/// own tenant, `application` recipients only on events without tenant, and
/// the delay bound.
fn admissible(
    reactor: &Reactor,
    effect: &Effect,
    tenant: Option<&Identity>,
) -> Result<(), &'static str> {
    let kind = match effect {
        Effect::Notify { .. } => EffectKind::Notify,
        Effect::Webhook { .. } => EffectKind::Webhook,
        Effect::Command { .. } => EffectKind::Command,
    };
    if !reactor.effects.contains(&kind) {
        return Err("effect_not_allowed");
    }
    match effect {
        Effect::Notify {
            recipients,
            delay_ms,
            ..
        } => {
            for recipient in recipients {
                match recipient {
                    Recipient::Address(_) if !reactor.allow_addresses => {
                        return Err("address_not_allowed");
                    }
                    Recipient::Tenant { id, .. } => match tenant {
                        None => return Err("tenant_required"),
                        Some(tenant) if tenant != id => return Err("invalid_recipient"),
                        Some(_) => {}
                    },
                    // Application roles span every tenant: a tenant event
                    // never notifies them.
                    Recipient::Application { .. } if tenant.is_some() => {
                        return Err("invalid_recipient");
                    }
                    _ => {}
                }
            }
            delay(*delay_ms)
        }
        Effect::Webhook { .. } => Ok(()),
        Effect::Command { tag, delay_ms, .. } => {
            if !reactor.commands.contains(tag) {
                return Err("command_not_allowed");
            }
            delay(*delay_ms)
        }
    }
}
fn delay(delay_ms: Option<u64>) -> Result<(), &'static str> {
    if delay_ms.is_some_and(|d| d > MAX_EFFECT_DELAY_MS) {
        Err("invalid_delay")
    } else {
        Ok(())
    }
}

/// Plan the batch's effects: what the host refuses itself, what the port's
/// pure check refuses, and the requests to write.
fn plan(
    reactor: &Reactor,
    effects: &dyn Effects,
    events: &[(StoredEvent, Option<Identity>)],
    reactions: Vec<Reaction>,
) -> Result<Plan, Failure> {
    let mut plan = Plan {
        accepted: Vec::new(),
        rejected: Vec::new(),
    };
    for ((event, tenant), reaction) in events.iter().zip(reactions) {
        for (index, effect) in reaction.effects.into_iter().enumerate() {
            let index = u8::try_from(index).map_err(|_| invalid())?;
            let kind = effect.kind();
            let reject = |code: String| Rejection {
                position: event.position,
                index,
                effect: kind,
                code,
            };
            if let Err(code) = admissible(reactor, &effect, tenant.as_ref()) {
                plan.rejected.push(reject(code.to_owned()));
                continue;
            }
            let body = match effect {
                Effect::Notify {
                    topic,
                    recipients,
                    channels,
                    data,
                    delay_ms,
                } => EffectBody::Notify {
                    topic,
                    recipients,
                    channels,
                    data,
                    delay_ms,
                    members: Vec::new(),
                },
                Effect::Webhook { event_type, data } => EffectBody::Webhook { event_type, data },
                Effect::Command {
                    tag,
                    stream,
                    payload,
                    expected_version,
                    delay_ms,
                } => EffectBody::Command {
                    job: CommandJob {
                        reactor: reactor.name.clone(),
                        position: event.position,
                        index,
                        event_id: event.event_id.clone(),
                        correlation_id: event.metadata.correlation_id.clone(),
                        tenant: tenant.clone(),
                        tag,
                        stream,
                        payload,
                        expected_version,
                    },
                    delay_ms,
                },
            };
            let request = EffectRequest {
                reactor: reactor.name.clone(),
                position: event.position,
                index,
                dedupe_key: format!(
                    "reaction:{}:{}:{index}",
                    reactor.name.as_str(),
                    event.event_id.as_str()
                ),
                tenant: tenant.clone(),
                correlation_id: event.metadata.correlation_id.clone(),
                event_id: event.event_id.clone(),
                accepted_unix_ms: event.accepted_unix_ms,
                body,
            };
            match effects.check(&request) {
                Ok(()) => plan.accepted.push(request),
                Err(failure) => plan.rejected.push(reject(failure.code)),
            }
        }
    }
    Ok(plan)
}

/// The batch's tenant memberships (AP §C.5 step 4), before any
/// transaction: each `(user, tenant)` pair of a notify effect on a tenant
/// event is asked once, and the confirmed users fill `members`. A
/// Directory failure fails the batch (transient).
fn memberships(
    store: &PostgresStore,
    bounds: &Bounds,
    effects: &dyn Effects,
    plan: &mut Plan,
) -> Result<(), Failure> {
    let mut pairs = BTreeSet::new();
    for request in &plan.accepted {
        if let (EffectBody::Notify { recipients, .. }, Some(tenant)) =
            (&request.body, &request.tenant)
        {
            for recipient in recipients {
                if let Recipient::User { id } = recipient {
                    pairs.insert((id.clone(), tenant.clone()));
                }
            }
        }
    }
    let mut confirmed = BTreeMap::new();
    for (user, tenant) in pairs {
        let member = store.step(bounds, effects.is_member(&user, &tenant))?;
        confirmed.insert((user, tenant), member);
    }
    for request in &mut plan.accepted {
        if let (
            EffectBody::Notify {
                recipients,
                members,
                ..
            },
            Some(tenant),
        ) = (&mut request.body, &request.tenant)
        {
            for recipient in recipients.iter() {
                if let Recipient::User { id } = recipient
                    && confirmed
                        .get(&(id.clone(), tenant.clone()))
                        .copied()
                        .unwrap_or(false)
                    && !members.contains(id)
                {
                    members.push(id.clone());
                }
            }
        }
    }
    Ok(())
}

/// How a batch failed.
enum Failed {
    /// The guest's own transient failure (`timeout`, `overloaded`, a
    /// transient trap): retried with backoff like [`Failed::Transient`],
    /// and after [`ISOLATE_AFTER`] consecutive ones on several events, one
    /// event at a time.
    GuestTransient(RunFailure),
    /// Retry the same range later, with backoff (`timeout`, `overloaded`,
    /// storage, the Directory, `unavailable`).
    Transient(RunFailure),
    /// The guest failed permanently (`malformed = false`) or broke the
    /// reply contract (`malformed = true`).
    Guest { run: RunFailure, malformed: bool },
}

/// One reactor run under its `r:` lock (AP §C.5): batches until the head,
/// lingering while wakes arrive, within the run's bounds. A failure is
/// recorded on the row and the run ends with the retry delay.
pub(crate) fn react(
    app: &Application,
    store: &PostgresStore,
    reactor: &Reactor,
    bounds: &Bounds,
) -> Result<ReactorOutcome, RunFailure> {
    let effects = store
        .options
        .effects
        .clone()
        .ok_or_else(|| Failure::permanent("reactor_effects_unavailable"))?;
    let Some(mut session) = store.lock_reactor(bounds, reactor)? else {
        return Ok(ReactorOutcome::Contended);
    };
    let lane = store.reactor_lane(reactor.name.as_str())?;
    let options = &store.shared().options;
    let batch = app.config().limits.batch_size as u64;
    // Whether the run already lingered once without new events since its
    // last batch: a wake that brought nothing ends it, so the interval's
    // wakes alone never keep a run (and its background slot) alive.
    let mut lingered = false;
    loop {
        let row = session.row()?;
        lane.halted.store(row.status == "halted", Ordering::SeqCst);
        app.instruments().observe_reactor(
            reactor.name.as_str(),
            row.checkpoint,
            row.status == "halted",
        );
        if row.status == "paused" {
            return Ok(ReactorOutcome::Waiting(None));
        }
        if row.retry_ms > 0 {
            return Ok(ReactorOutcome::Waiting(Some(Duration::from_millis(
                row.retry_ms,
            ))));
        }
        let head = session.head()?;
        app.instruments().observe_head(head);
        if row.checkpoint >= head {
            // Caught up: a background run stays while wakes keep
            // arriving, within the run; a request returns.
            if !bounds.background
                || lingered
                || Instant::now() >= bounds.soft
                || !store.step(bounds, async {
                    Ok(tokio::time::timeout(LINGER, lane.wake.notified())
                        .await
                        .is_ok())
                })?
            {
                return Ok(ReactorOutcome::Finished { behind: false });
            }
            lingered = true;
            continue;
        }
        lingered = false;
        if Instant::now() >= bounds.soft {
            return Ok(ReactorOutcome::Finished { behind: true });
        }
        let narrow = lane.narrow.load(Ordering::SeqCst);
        let limit = if narrow > row.checkpoint {
            1
        } else {
            batch.min(head - row.checkpoint)
        };
        let (events, traces, tenants) = session.feed(row.checkpoint, limit)?;
        let end = events.last().map_or(row.checkpoint, |e| e.position);
        let read = events.len();
        let first = events.first().map(|e| (e.position, e.accepted_unix_ms));
        let kept: Vec<(StoredEvent, Option<Identity>)> = events
            .into_iter()
            .zip(tenants)
            .filter(|(event, _)| {
                reactor.events.is_empty() || reactor.events.contains(&event.event.tag)
            })
            .map(|(event, tenant)| {
                let tenant = tenant
                    .map(Identity::new)
                    .transpose()
                    .map_err(|_| invalid())?;
                Ok((event, tenant))
            })
            .collect::<Result<_, Failure>>()?;
        if let Some((position, accepted)) = first {
            app.instruments()
                .observe_reactor_feed(reactor.name.as_str(), position, accepted);
        }
        if kept.is_empty() {
            // Nothing to react to: advance with no guest call.
            let plan = Plan {
                accepted: Vec::new(),
                rejected: Vec::new(),
            };
            match session.commit(&row, end, plan, None, effects.clone()) {
                Ok(_) => continue,
                Err(failure) if failure.code == "reactor_conflict" => continue,
                Err(failure) => {
                    let wait = fail(
                        &session,
                        reactor,
                        &row,
                        Failed::Transient(failure.into()),
                        options,
                    )?;
                    return Ok(ReactorOutcome::Waiting(Some(wait)));
                }
            }
        }
        let mut run = app.instruments().react(reactor.name.as_str());
        run.batch(&traces);
        let span = run.span().clone();
        let outcome =
            span.in_scope(|| batch_of(app, &mut session, reactor, &row, end, kept, &effects));
        match outcome {
            Ok((written, rejected)) => {
                let kinds: Vec<&'static str> = rejected.iter().map(|r| r.effect).collect();
                run.finish_accepted(read, end, &written, &kinds);
                for rejection in &rejected {
                    merezha_telemetry::event!(
                        WARN,
                        "reactor.effect_rejected",
                        reactor = reactor.name.as_str(),
                        position = rejection.position,
                        index = rejection.index,
                        reason = rejection.effect,
                        code = rejection.code.as_str()
                    );
                }
                app.instruments()
                    .observe_reactor(reactor.name.as_str(), end, false);
                lane.halted.store(false, Ordering::SeqCst);
            }
            Err(Failed::Transient(failure)) if failure.failure.code == "reactor_conflict" => {
                run.finish_failed(&failure, false);
            }
            Err(Failed::Guest { run: failure, .. }) if read > 1 => {
                // Isolate the poison event: the same range, one event at a time.
                run.finish_failed(&failure, false);
                lane.narrow.store(end, Ordering::SeqCst);
            }
            Err(Failed::Guest {
                run: failure,
                malformed,
            }) if !malformed && reactor.on_error == OnError::Skip => {
                run.finish_failed(&failure, false);
                let plan = Plan {
                    accepted: Vec::new(),
                    rejected: Vec::new(),
                };
                let skipped = session.commit(
                    &row,
                    end,
                    plan,
                    Some((end, failure.failure.code.clone())),
                    effects.clone(),
                );
                match skipped {
                    Ok(_) => {
                        app.instruments().reactor_skipped(
                            reactor.name.as_str(),
                            merezha_telemetry::names::Reason::OnErrorSkip,
                        );
                        merezha_telemetry::event!(
                            INFO,
                            "reactor.skipped",
                            reactor = reactor.name.as_str(),
                            position = end,
                            reason = "on_error_skip",
                            code = failure.log_code()
                        );
                    }
                    Err(conflict) if conflict.code == "reactor_conflict" => {}
                    Err(other) => {
                        let wait = fail(
                            &session,
                            reactor,
                            &row,
                            Failed::Transient(other.into()),
                            options,
                        )?;
                        return Ok(ReactorOutcome::Waiting(Some(wait)));
                    }
                }
            }
            Err(Failed::GuestTransient(failure))
                if read > 1 && row.failures.saturating_add(1) >= ISOLATE_AFTER =>
            {
                // An event that keeps failing transiently must not stall
                // the events before it: from now on one event at a time,
                // so the healthy prefix commits and the failing event is
                // the stuck one an operator can `skip`.
                run.finish_failed(&failure, false);
                let code = failure.log_code().to_owned();
                let wait = fail(
                    &session,
                    reactor,
                    &row,
                    Failed::GuestTransient(failure),
                    options,
                )?;
                lane.narrow.store(end, Ordering::SeqCst);
                merezha_telemetry::event!(
                    WARN,
                    "reactor.isolating",
                    reactor = reactor.name.as_str(),
                    position = row.checkpoint + 1,
                    reason = "transient_failures",
                    code = code.as_str()
                );
                return Ok(ReactorOutcome::Waiting(Some(wait)));
            }
            Err(failed) => {
                let halting = matches!(failed, Failed::Guest { .. });
                match &failed {
                    Failed::Transient(f)
                    | Failed::GuestTransient(f)
                    | Failed::Guest { run: f, .. } => run.finish_failed(f, halting),
                }
                let wait = fail(&session, reactor, &row, failed, options)?;
                if halting {
                    lane.halted.store(true, Ordering::SeqCst);
                    app.instruments()
                        .observe_reactor(reactor.name.as_str(), row.checkpoint, true);
                }
                return Ok(ReactorOutcome::Waiting(Some(wait)));
            }
        }
    }
}
/// Record a failed batch on the row: a later retry, and for a guest
/// failure on a single event a halt at its position, logged once. Returns
/// the backoff recorded.
fn fail(
    session: &Session<'_>,
    reactor: &Reactor,
    row: &Row,
    failed: Failed,
    options: &super::BackgroundOptions,
) -> Result<Duration, RunFailure> {
    let delay = backoff(
        options.reactor_retry.0,
        options.reactor_retry.1,
        row.failures.saturating_add(1),
    );
    let position = row.checkpoint + 1;
    match failed {
        Failed::Transient(run) | Failed::GuestTransient(run) => {
            session.record_failure(&run.failure, row.checkpoint, delay, None)?;
        }
        Failed::Guest { run, malformed } => {
            session.record_failure(&run.failure, row.checkpoint, delay, Some(position))?;
            if row.status != "halted" {
                merezha_telemetry::event!(
                    ERROR,
                    "reactor.halted",
                    reactor = reactor.name.as_str(),
                    position,
                    reason = if malformed {
                        "malformed_reply"
                    } else {
                        "guest_failed"
                    },
                    code = run.log_code()
                );
            }
        }
    }
    Ok(delay)
}
/// One batch with matching events: the guest call, both reply tiers, the
/// memberships and the checkpoint transaction. Returns the types of the
/// effects written and every rejection.
fn batch_of(
    app: &Application,
    session: &mut Session<'_>,
    reactor: &Reactor,
    row: &Row,
    end: u64,
    kept: Vec<(StoredEvent, Option<Identity>)>,
    effects: &Arc<dyn Effects>,
) -> Result<(Vec<&'static str>, Vec<Rejection>), Failed> {
    let limits = &app.config().limits;
    let deadline = session
        .bounds
        .hard
        .min(Instant::now() + Duration::from_millis(limits.timeout_ms));
    let guest = app
        .guest()
        .session()
        .map_err(|failure| Failed::Transient(failure.into()))?;
    let events: Vec<StoredEvent> = kept.iter().map(|(event, _)| event.clone()).collect();
    let reply = app
        .call_guest(
            guest.as_ref(),
            reactor.service.as_str(),
            Operation::React {
                reactor: reactor.name.clone(),
                events: events.clone(),
            },
            deadline,
        )
        .map_err(|failure| {
            let run = RunFailure::from_guest_call(failure);
            match run.failure.classification {
                Classification::Permanent => Failed::Guest {
                    malformed: matches!(
                        run.failure.code.as_str(),
                        "invalid_response" | "protocol_version"
                    ),
                    run,
                },
                _ => Failed::GuestTransient(run),
            }
        })?;
    let reactions =
        reactions(reply, &events, limits.max_payload_bytes).map_err(|failure| Failed::Guest {
            run: failure.into(),
            malformed: true,
        })?;
    let mut plan = plan(reactor, effects.as_ref(), &kept, reactions)
        .map_err(|f| Failed::Transient(f.into()))?;
    memberships(session.store, session.bounds, effects.as_ref(), &mut plan)
        .map_err(|f| Failed::Transient(f.into()))?;
    let mut rejected = plan.rejected.clone();
    let (written, added) = session
        .commit(row, end, plan, None, effects.clone())
        .map_err(|f| Failed::Transient(f.into()))?;
    rejected.extend(added);
    rejected.sort_by_key(|r| (r.position, r.index));
    Ok((written, rejected))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_equal_jitter_within_its_bounds() {
        let (base, max) = (Duration::from_secs(1), Duration::from_secs(300));
        for failures in 1..30 {
            let ceiling = base.saturating_mul(1u32 << (failures - 1).min(20)).min(max);
            for _ in 0..20 {
                let delay = backoff(base, max, failures);
                assert!(
                    delay >= ceiling / 2 && delay <= ceiling,
                    "{failures}: {delay:?}"
                );
            }
        }
        assert_eq!(backoff(Duration::ZERO, Duration::ZERO, 3), Duration::ZERO);
    }
}
