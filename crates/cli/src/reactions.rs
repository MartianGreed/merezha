//! The platform side of reactors (AP §C.5, R4): the application's
//! [`Effects`] port over the platform's `Notifier`, `Outbox` and
//! `Directory`, bound to the source `application`, and the
//! `application.command` job handler that runs command effects as the
//! reactor's system principal.
//!
//! `merezha-application` never depends on `merezha-platform`, so the
//! application defines the port and this adapter implements it, as
//! [`crate::application_http::EdgeCaller`] does for route permissions.
use merezha_application::{
    Application,
    postgres::{CommandJob, EffectBody, EffectRequest, Effects},
};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    AppContext, BoxFuture,
    ports::{
        Address, Channel, ChannelSelection, Correlation, DedupeKey, Directory, EmailAddress,
        EventId, JobContext, JobFailure, JobHandler, MessageContext, NotificationRequest, Notifier,
        OnDuplicate, OutboxRecord, OutboxTarget, PhoneNumber, Recipient, SourceOutbox,
        WebhookEvent,
    },
};
use merezha_postgres::Tx;
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

/// The effect ports of one platform host, bound to the source
/// `application`. Without the notifications block the notifier is the
/// capture fallback, which delivers nothing: inside the checkpoint
/// transaction it only checks each effect, and the redacted record is kept
/// and logged once the transaction committed ([`Effects::committed`]), so
/// a batch rolled back and run again logs once.
pub struct PlatformEffects {
    notifier: Arc<dyn Notifier>,
    outbox: SourceOutbox,
    directory: Arc<dyn Directory>,
}
impl PlatformEffects {
    /// From the application module's context, which may bind its own name
    /// `application`.
    pub fn new(ctx: &AppContext) -> Result<Self, Failure> {
        Ok(Self {
            notifier: ctx.notifier(merezha_application::postgres::SOURCE)?,
            outbox: ctx.outbox(merezha_application::postgres::SOURCE)?,
            directory: ctx.directory().clone(),
        })
    }
}

fn timestamp(unix_ms: u64) -> Result<jiff::Timestamp, Failure> {
    i64::try_from(unix_ms)
        .ok()
        .and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok())
        .ok_or_else(|| Failure::permanent("invalid_delay"))
}
/// Now plus `delay_ms`, `None` without a delay.
fn after(delay_ms: Option<u64>) -> Result<Option<jiff::Timestamp>, Failure> {
    delay_ms
        .map(|delay| {
            jiff::Timestamp::now()
                .checked_add(Duration::from_millis(delay))
                .map_err(|_| Failure::permanent("invalid_delay"))
        })
        .transpose()
}
fn recipient(recipient: &merezha_domain::Recipient) -> Result<Recipient, Failure> {
    use merezha_domain::Recipient as Guest;
    Ok(match recipient {
        Guest::User { id } => Recipient::User { id: id.clone() },
        Guest::Tenant { id, roles } => Recipient::Tenant {
            id: id.clone(),
            roles: roles.clone(),
        },
        Guest::Application { roles } => Recipient::Application {
            roles: roles.clone(),
        },
        Guest::Address(merezha_domain::Address::Email(email)) => {
            Recipient::Address(Address::Email {
                address: EmailAddress::new(email.email.clone())?,
                name: None,
                user: None,
                locale: None,
            })
        }
        Guest::Address(merezha_domain::Address::Phone(phone)) => {
            Recipient::Address(Address::Phone {
                e164: PhoneNumber::new(phone.phone.clone())?,
                user: None,
                locale: None,
            })
        }
    })
}
fn channel(channel: merezha_domain::Channel) -> Channel {
    match channel {
        merezha_domain::Channel::Email => Channel::Email,
        merezha_domain::Channel::Sms => Channel::Sms,
        merezha_domain::Channel::Push => Channel::Push,
        merezha_domain::Channel::Inbox => Channel::Inbox,
    }
}
/// The notification request of a notify effect, or `None` for another
/// effect type. Every failure here is permanent: the effect is rejected.
fn notification(request: &EffectRequest) -> Result<Option<NotificationRequest>, Failure> {
    let EffectBody::Notify {
        topic,
        recipients,
        channels,
        data,
        delay_ms,
        members,
    } = &request.body
    else {
        return Ok(None);
    };
    Ok(Some(NotificationRequest {
        topic: topic.clone(),
        recipients: recipients.iter().map(recipient).collect::<Result<_, _>>()?,
        channels: match channels {
            Some(only) => ChannelSelection::Only(only.iter().copied().map(channel).collect()),
            None => ChannelSelection::TopicDefault,
        },
        template: None,
        data: data.clone(),
        secret_data: serde_json::Map::new(),
        locale: None,
        dedupe_key: DedupeKey::new(request.dedupe_key.clone())?,
        tenant: request.tenant.clone(),
        members: members.clone(),
        not_before: after(*delay_ms)?,
        expires_at: None,
        collapse_key: None,
        correlation: Correlation {
            correlation_id: request.correlation_id.clone(),
            causation_id: Some(request.event_id.clone()),
        },
    }))
}
/// The webhook event of a webhook effect, or `None` for another effect
/// type. Every failure here is permanent: the effect is rejected.
fn webhook(request: &EffectRequest) -> Result<Option<WebhookEvent>, Failure> {
    let EffectBody::Webhook { event_type, data } = &request.body else {
        return Ok(None);
    };
    Ok(Some(WebhookEvent {
        tenant: request.tenant.clone(),
        event_type: event_type.clone(),
        source_event_id: EventId::new(request.dedupe_key.clone())?,
        occurred_at: timestamp(request.accepted_unix_ms)?,
        data: data.clone(),
    }))
}
/// A permanent failure keeps its code, so the effect is rejected with it;
/// anything else rolls the checkpoint transaction back.
fn notify_failure(error: merezha_platform::ports::NotifyError) -> Failure {
    Failure::from(error)
}

impl Effects for PlatformEffects {
    fn check(&self, request: &EffectRequest) -> Result<(), Failure> {
        match notification(request)? {
            Some(notification) => self
                .notifier
                .validate(&notification)
                .map_err(notify_failure),
            None => Ok(()),
        }
    }
    fn members<'a>(
        &'a self,
        tenant: &'a Identity,
        users: &'a [Identity],
    ) -> BoxFuture<'a, Result<Vec<Identity>, Failure>> {
        self.directory.members_among(tenant, users)
    }
    fn write<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        request: &'a EffectRequest,
    ) -> BoxFuture<'a, Result<(), Failure>> {
        Box::pin(async move {
            match &request.body {
                EffectBody::Notify { .. } => {
                    let notification = notification(request)?
                        .ok_or_else(|| Failure::permanent("invalid_effect"))?;
                    // The capture fallback keeps its record after commit.
                    if let Some(capture) = self.notifier.as_capture() {
                        return capture.check(&notification).map_err(notify_failure);
                    }
                    self.notifier
                        .request(tx, notification)
                        .await
                        .map(|_| ())
                        .map_err(notify_failure)
                }
                EffectBody::Webhook { .. } => {
                    let event =
                        webhook(request)?.ok_or_else(|| Failure::permanent("invalid_effect"))?;
                    if self.notifier.as_capture().is_some() {
                        return Ok(());
                    }
                    self.notifier
                        .emit_webhook(tx, event)
                        .await
                        .map(|_| ())
                        .map_err(notify_failure)
                }
                EffectBody::Command { job, delay_ms } => {
                    let record = OutboxRecord {
                        dedupe_key: DedupeKey::new(request.dedupe_key.clone())?,
                        target: OutboxTarget::Job {
                            queue: Identity::new(merezha_application::postgres::COMMAND_QUEUE)?,
                            kind: Identity::new(merezha_application::postgres::COMMAND_KIND)?,
                        },
                        payload: serde_json::to_value(job)
                            .map_err(|_| Failure::permanent("serialization_failed"))?,
                        contains_secret: false,
                        run_at: after(*delay_ms)?,
                        priority: 0,
                        context: MessageContext {
                            correlation_id: request.correlation_id.clone(),
                            causation_id: Some(request.event_id.clone()),
                            tenant: request.tenant.clone(),
                        },
                    };
                    // The outbox validates before it writes: a permanent
                    // refusal (a payload over 64 KiB) rejects the effect.
                    self.outbox
                        .append(tx, std::slice::from_ref(&record), OnDuplicate::Ignore)
                        .await
                        .map(|_| ())
                }
            }
        })
    }
    /// The capture fallback's records of the effects a committed batch
    /// wrote; nothing with the notifications block, which wrote them in
    /// the transaction.
    fn committed(&self, written: &[EffectRequest]) {
        let Some(capture) = self.notifier.as_capture() else {
            return;
        };
        for request in written {
            // Both were built and checked inside the transaction.
            if let Ok(Some(notification)) = notification(request) {
                let _ = capture.capture(notification);
            } else if let Ok(Some(event)) = webhook(request) {
                capture.capture_webhook(&event);
            }
        }
    }
}

/// The `application.command` handler (AP §C.5): the command effect as the
/// reactor's system principal, inside `spawn_blocking`. A receipt, new or
/// replayed, succeeds; `concurrency_conflict` and other conflicts and
/// transient failures retry; a permanent failure (including
/// `idempotency_mismatch` and `unauthorized`) dead-letters.
pub struct CommandEffects {
    application: Arc<OnceLock<Application>>,
    /// Whether routes declaring neither `permission` nor `access` admit
    /// reactors: only under the loopback allow-all policy.
    undeclared: bool,
}
impl CommandEffects {
    pub fn new(application: Arc<OnceLock<Application>>, undeclared: bool) -> Self {
        Self {
            application,
            undeclared,
        }
    }
}
impl JobHandler for CommandEffects {
    fn handle<'a>(
        &'a self,
        _: JobContext,
        payload: serde_json::Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let job: CommandJob = serde_json::from_value(payload)
                .map_err(|_| JobFailure::permanent("invalid_job_payload"))?;
            // The module set the application before the jobs runtime
            // started; give the job back to another instance otherwise.
            let application = self
                .application
                .get()
                .cloned()
                .ok_or_else(JobFailure::release)?;
            let undeclared = self.undeclared;
            let result = merezha_telemetry::spawn_blocking_in_span(move || {
                application.command_effect(&job, undeclared)
            })
            .await
            .map_err(|_| JobFailure::transient("internal", None))?;
            // The classification drives retry: conflict and transient
            // retry, permanent dead-letters. `idempotency_mismatch` is a
            // conflict for a caller, but here the job's own key names
            // another command: retrying cannot succeed (AP §C.5).
            result.map(|_| ()).map_err(|failure| {
                if failure.code == "idempotency_mismatch" {
                    JobFailure::permanent(&failure.code)
                } else {
                    JobFailure::from(failure)
                }
            })
        })
    }
}
