//! Webhook fan-out (NT §5.5): `notify.webhook_event`, on queue
//! `notify.expand`, in one transaction.
//!
//! 1. Insert the event with `ON CONFLICT DO NOTHING`; an existing row
//!    means the fan-out already committed: done.
//! 2. Serialize the envelope once, `{"type","timestamp","data"}`, at most
//!    `webhooks.max_payload_bytes`; every attempt signs exactly these bytes
//!    (invariant 9).
//! 3. Insert one delivery per matching active endpoint of the event's
//!    registry (`COALESCE(tenant_id, '')` equal to the event's tenant, or
//!    `''` for a tenantless event: the application-level endpoints) and
//!    append its job record, at most 256 per append.
//!
//! [`fan_out`] takes an [`Event`], so NT-N8's module-event subscription
//! (`notify.module_event`) maps AP's envelope onto one and reuses it.
use super::{DELIVER_KIND, DELIVER_QUEUE, DeliverJob, delivery_id, sql};
use crate::{handler::Outcome, notifier::Core};
use merezha_domain::{Failure, Identity};
use merezha_platform::ports::{
    DedupeKey, MAX_RECORDS_PER_APPEND, MessageContext, OnDuplicate, OutboxRecord, OutboxTarget,
    SourceOutbox,
};
use merezha_postgres::{Database, Tx, classify};
use serde::Deserialize;
use serde_json::Value;
use std::time::Instant;
use uuid::Uuid;

/// Most endpoints one event reaches: the largest registry the
/// configuration allows.
pub const MAX_FANOUT: i64 = 1000;

/// A webhook event to fan out. `Debug` shows ids and the type only.
#[derive(Clone)]
pub struct Event {
    pub id: Uuid,
    pub source: String,
    pub source_event_id: String,
    pub tenant: Option<String>,
    pub event_type: String,
    pub occurred_at: jiff::Timestamp,
    pub data: Value,
}
impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Event")
            .field("id", &self.id)
            .field("source", &self.source)
            .field("event_type", &self.event_type)
            .finish_non_exhaustive()
    }
}
impl Event {
    /// The frozen envelope: `{"type":…,"timestamp":"<RFC 3339>","data":…}`.
    pub fn envelope(&self) -> Vec<u8> {
        let text = |value: &str| Value::String(value.to_owned()).to_string();
        format!(
            "{{\"type\":{},\"timestamp\":{},\"data\":{}}}",
            text(&self.event_type),
            text(&self.occurred_at.to_string()),
            self.data
        )
        .into_bytes()
    }
}

/// The `notify.webhook_event` payload `emit_webhook` writes (version 1).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmitPayload {
    pub v: u8,
    #[serde(with = "crate::payload::uuid_text")]
    pub webhook_event_id: Uuid,
    pub source: String,
    pub source_event_id: String,
    pub tenant: Option<String>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub occurred_at: jiff::Timestamp,
    pub data: Value,
}
impl From<EmitPayload> for Event {
    fn from(payload: EmitPayload) -> Self {
        Self {
            id: payload.webhook_event_id,
            source: payload.source,
            source_event_id: payload.source_event_id,
            tenant: payload.tenant,
            event_type: payload.event_type,
            occurred_at: payload.occurred_at,
            data: payload.data,
        }
    }
}

/// The job record of one webhook delivery generation.
pub(crate) fn job_record(
    job: &DeliverJob,
    correlation: &Identity,
    tenant: Option<&str>,
) -> Result<OutboxRecord, Failure> {
    Ok(OutboxRecord {
        dedupe_key: DedupeKey::new(format!(
            "webhook:{}:g{}",
            job.webhook_delivery_id, job.generation
        ))?,
        target: OutboxTarget::Job {
            queue: Identity::new(DELIVER_QUEUE)?,
            kind: Identity::new(DELIVER_KIND)?,
        },
        payload: job.to_value(),
        contains_secret: false,
        run_at: None,
        priority: 0,
        context: MessageContext {
            correlation_id: correlation.clone(),
            causation_id: None,
            tenant: tenant.and_then(|t| Identity::new(t).ok()),
        },
    })
}

/// Insert `deliveries` (`(delivery, endpoint)`) of `event` and append
/// their job records, in `tx`. The deliveries that were new.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_deliveries(
    db: &Database,
    outbox: &SourceOutbox,
    tx: &mut Tx<'_>,
    event: Uuid,
    deliveries: &[(Uuid, Uuid)],
    correlation: &Identity,
    tenant: Option<&str>,
    now: jiff::Timestamp,
) -> Result<Vec<Uuid>, Failure> {
    let (ids, endpoints): (Vec<Uuid>, Vec<Uuid>) = deliveries.iter().copied().unzip();
    let rows = db
        .query(
            tx,
            &sql::DELIVERIES_INSERT,
            &[&event, &now, &ids, &endpoints],
        )
        .await
        .map_err(|e| classify(&e))?;
    let mut records = Vec::with_capacity(rows.len());
    let mut created = Vec::with_capacity(rows.len());
    for row in &rows {
        let job = DeliverJob {
            webhook_delivery_id: row.get(0),
            endpoint_id: row.get(1),
            generation: 0,
            busy: 0,
            busy_since: None,
        };
        created.push(job.webhook_delivery_id);
        records.push(job_record(&job, correlation, tenant)?);
    }
    for chunk in records.chunks(MAX_RECORDS_PER_APPEND) {
        outbox.append(tx, chunk, OnDuplicate::Ignore).await?;
    }
    Ok(created)
}

/// Fan `event` out to its registry's matching endpoints (see the module
/// docs). `correlation` is the event's correlation id, which the delivery
/// jobs carry on.
pub async fn fan_out(
    core: &Core,
    event: Event,
    correlation: Identity,
    deadline: Instant,
) -> Outcome {
    let Some(sender) = core.webhooks.clone() else {
        return Outcome::Permanent(super::code::WEBHOOKS_DISABLED.to_owned());
    };
    let body = event.envelope();
    if body.len() > sender.settings.max_payload_bytes as usize {
        return Outcome::Permanent("data_too_large".to_owned());
    }
    let db = core.database.clone();
    let outbox = core.outbox.clone();
    let now = core.now();
    let result = db
        .clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let inserted = db
                    .query_opt(
                        tx,
                        &sql::EVENT_INSERT,
                        &[
                            &event.id,
                            &event.source,
                            &event.source_event_id,
                            &event.tenant,
                            &event.event_type,
                            &event.occurred_at,
                            &body,
                            &now,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                if inserted.is_none() {
                    return Ok(0);
                }
                let registry = event.tenant.clone().unwrap_or_default();
                let endpoints: Vec<Uuid> = db
                    .query(
                        tx,
                        &sql::FANOUT_ENDPOINTS,
                        &[&registry, &event.event_type, &MAX_FANOUT],
                    )
                    .await
                    .map_err(|e| classify(&e))?
                    .iter()
                    .map(|row| row.get(0))
                    .collect();
                let deliveries: Vec<(Uuid, Uuid)> = endpoints
                    .iter()
                    .map(|endpoint| (delivery_id(event.id, *endpoint), *endpoint))
                    .collect();
                let created = insert_deliveries(
                    &db,
                    &outbox,
                    tx,
                    event.id,
                    &deliveries,
                    &correlation,
                    event.tenant.as_deref(),
                    now,
                )
                .await?;
                Ok(created.len())
            })
        })
        .await;
    match result {
        Ok(_) => Outcome::Done,
        Err(failure) => Outcome::retry(&failure.code),
    }
}

/// `notify.webhook_event`.
pub struct Handler(pub crate::notifier::Slot);
impl merezha_platform::ports::JobHandler for Handler {
    fn handle<'a>(
        &'a self,
        ctx: merezha_platform::ports::JobContext,
        payload: Value,
    ) -> merezha_platform::BoxFuture<'a, Result<(), merezha_platform::ports::JobFailure>> {
        Box::pin(async move {
            let Some(core) = self.0.get().cloned() else {
                return Err(merezha_platform::ports::JobFailure::transient(
                    "unavailable",
                    None,
                ));
            };
            let Ok(payload) = serde_json::from_value::<EmitPayload>(payload) else {
                return Err(merezha_platform::ports::JobFailure::permanent(
                    "invalid_job_payload",
                ));
            };
            if payload.v != 1 {
                return Err(merezha_platform::ports::JobFailure::permanent(
                    "invalid_job_payload",
                ));
            }
            fan_out(
                &core,
                payload.into(),
                ctx.correlation_id.clone(),
                ctx.deadline.into_std(),
            )
            .await
            .into_result()
        })
    }
}
