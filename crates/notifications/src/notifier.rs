//! The `Notifier` port (NT §3.1, R2): `validate`, `request` and
//! `emit_webhook` (outbox side), bound to the caller's source.
//!
//! `request` validates, applies the membership rule of R4 to
//! `request.members` and appends one `notify.expand` job record in the
//! caller's transaction. It calls no other port and takes no other pooled
//! connection, so it completes on a pool whose only connection the caller's
//! transaction holds. `emit_webhook` appends one `notify.webhook_event`
//! record; its fan-out handler arrives with NT-N7, so until then those jobs
//! wait in `notify.expand` unleased.
use crate::{
    Checked,
    config::Settings,
    faults::Faults,
    ids,
    keys::Keys,
    metrics::Metrics,
    payload::ExpandPayload,
    templates::{RESERVED_KEYS, RenderContext, TemplateRegistry},
    topics::Topic,
};
use merezha_domain::Identity;
use merezha_platform::{
    BoxFuture, Clock, Random,
    ports::{
        Channel, ChannelSelection, DedupeKey, Directory, MAX_COLLAPSE_KEY, MessageContext,
        NotificationHandle, NotificationRequest, Notifier, NotifierFactory, NotifyError,
        OnDuplicate, OutboxRecord, OutboxTarget, Recipient, Source, SourceOutbox, WebhookEvent,
        WebhookEventHandle, check_members,
    },
};
use merezha_postgres::{Database, Tx};
use serde_json::{Map, Value};
use std::{
    sync::{Arc, OnceLock},
    time::SystemTime,
};

/// Queue and kind of expansion jobs (NT §2.5).
pub const EXPAND_QUEUE: &str = "notify.expand";
pub const EXPAND_KIND: &str = "notify.expand";
/// Kind of webhook fan-out jobs, handled from NT-N7.
pub const WEBHOOK_EVENT_KIND: &str = "notify.webhook_event";
/// Event types other modules declare (NT §5.5) and the built-in test event.
pub const MODULE_EVENT_TYPES: [&str; 4] = [
    "content.entry_published",
    "content.entry_unpublished",
    "content.entry_deleted",
    "webhook.test",
];

/// Everything the block's handlers and routes share once it started.
pub struct Core {
    pub settings: Arc<Settings>,
    pub registry: Arc<TemplateRegistry>,
    pub keys: Keys,
    /// Scoped to the module.
    pub database: Database,
    /// Source `notifications`.
    pub outbox: SourceOutbox,
    pub directory: Arc<dyn Directory>,
    pub clock: Arc<dyn Clock>,
    pub random: Arc<dyn Random>,
    pub metrics: Metrics,
    pub faults: Faults,
    /// The email driver, `None` while it is `disabled` or not yet built in
    /// this slice (Brevo, NT-N3).
    pub email: Option<Arc<dyn crate::drivers::EmailDriver>>,
    /// The SMS driver (capture, or Brevo from NT-N4), `None` while it is
    /// `disabled`.
    pub sms: Option<Arc<dyn crate::drivers::SmsDriver>>,
    /// The Web Push sender, `None` without `[modules.notifications.push]`.
    pub push: Option<Arc<crate::push::PushSender>>,
}
impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core").finish_non_exhaustive()
    }
}
impl Core {
    pub fn now(&self) -> jiff::Timestamp {
        timestamp(self.clock.now())
    }
    pub fn topic(&self, name: &str) -> Option<&Topic> {
        self.settings.topics.get(name)
    }
}
pub(crate) fn timestamp(time: SystemTime) -> jiff::Timestamp {
    jiff::Timestamp::try_from(time).unwrap_or(jiff::Timestamp::UNIX_EPOCH)
}

/// The core, set once the block started.
pub type Slot = Arc<OnceLock<Arc<Core>>>;

/// The block's [`NotifierFactory`]: installed in every context before any
/// module starts ([`merezha_platform::Module::notifier`]); its notifiers
/// answer `unavailable` until the block started.
#[derive(Clone, Debug)]
pub struct BlockNotifiers {
    slot: Slot,
    checked: Arc<OnceLock<Arc<Checked>>>,
}
impl BlockNotifiers {
    pub(crate) fn new(slot: Slot, checked: Arc<OnceLock<Arc<Checked>>>) -> Self {
        Self { slot, checked }
    }
    /// The notifier for `source`, as the host binds it.
    pub fn notifier(&self, source: Source) -> BoundNotifier {
        BoundNotifier {
            source,
            slot: self.slot.clone(),
            checked: self.checked.clone(),
        }
    }
}
impl NotifierFactory for BlockNotifiers {
    fn bind(&self, source: &Source) -> Arc<dyn Notifier> {
        Arc::new(self.notifier(source.clone()))
    }
}

/// One source's notifier.
#[derive(Clone, Debug)]
pub struct BoundNotifier {
    source: Source,
    slot: Slot,
    checked: Arc<OnceLock<Arc<Checked>>>,
}

/// A request that passed [`validate`]: its template and channels.
#[derive(Debug)]
pub(crate) struct Valid<'a> {
    pub topic: &'a Topic,
    pub template: String,
    pub channels: Vec<Channel>,
}

/// The checks of `validate`: topics, templates, channels and limits, then a
/// dry render of every requested channel in the default locale with strict
/// undefined. No I/O.
pub(crate) fn validate<'a>(
    settings: &'a Settings,
    registry: &TemplateRegistry,
    request: &NotificationRequest,
) -> Result<Valid<'a>, NotifyError> {
    let topic = settings
        .topics
        .get(request.topic.as_str())
        .ok_or(NotifyError::UnknownTopic)?;
    let template = request
        .template
        .as_ref()
        .map_or(topic.template.as_str(), Identity::as_str)
        .to_owned();
    let info = registry
        .get(&template)
        .ok_or(NotifyError::UnknownTemplate)?;
    let channels = match &request.channels {
        ChannelSelection::TopicDefault => topic.default_channels.clone(),
        ChannelSelection::Only(only) => {
            let mut chosen = Vec::new();
            for channel in only {
                if !topic.channels.contains(channel) {
                    return Err(NotifyError::ChannelNotAllowed);
                }
                if !chosen.contains(channel) {
                    chosen.push(*channel);
                }
            }
            if chosen.is_empty() {
                return Err(NotifyError::ChannelNotAllowed);
            }
            chosen
        }
    };
    if request.recipients.len() > settings.limits.max_recipients {
        return Err(NotifyError::TooManyRecipients);
    }
    if request.recipients.is_empty() {
        return Err(NotifyError::InvalidRecipient);
    }
    for recipient in &request.recipients {
        let valid = match recipient {
            Recipient::Application { roles } => !roles.is_empty(),
            Recipient::Tenant { id, .. } => request.tenant.as_ref() == Some(id),
            Recipient::Address(_) | Recipient::User { .. } => true,
        };
        if !valid {
            return Err(NotifyError::InvalidRecipient);
        }
    }
    if RESERVED_KEYS.iter().any(|k| request.data.contains_key(*k)) {
        return Err(NotifyError::ReservedDataKey);
    }
    let size = Value::Object(request.data.clone()).to_string().len()
        + Value::Object(request.secret_data.clone()).to_string().len();
    if size > settings.limits.max_data_bytes
        || request
            .collapse_key
            .as_ref()
            .is_some_and(|k| k.len() > MAX_COLLAPSE_KEY)
    {
        return Err(NotifyError::DataTooLarge);
    }
    let default = registry.default_locale().to_owned();
    let available = info.channels(registry, &default);
    for channel in &channels {
        if !available.contains(channel) {
            return Err(NotifyError::RenderFailed { channel: *channel });
        }
        let unsubscribe = registry.preview_unsubscribe(topic.category, *channel);
        let context = RenderContext {
            topic: &topic.name,
            category: topic.category,
            notification_id: uuid::Uuid::nil(),
            data: &request.data,
            secret: &request.secret_data,
            recipient_name: None,
            timezone: None,
            unsubscribe: unsubscribe.as_deref(),
        };
        registry
            .render(&template, *channel, &default, &context)
            .map_err(|_| NotifyError::RenderFailed { channel: *channel })?;
    }
    Ok(Valid {
        topic,
        template,
        channels,
    })
}

impl BoundNotifier {
    pub fn source(&self) -> &Source {
        &self.source
    }
    fn core(&self) -> Result<Arc<Core>, NotifyError> {
        self.slot.get().cloned().ok_or(NotifyError::Unavailable)
    }
    async fn append_request(
        &self,
        tx: &mut Tx<'_>,
        request: NotificationRequest,
    ) -> Result<NotificationHandle, NotifyError> {
        let core = self.core()?;
        let source = self.source.as_str();
        let topic = request.topic.as_str().to_owned();
        let notification_id = ids::notification_id(source, request.dedupe_key.as_str());
        let checked = validate(&core.settings, &core.registry, &request)
            .and_then(|valid| check_members(&self.source, &request).map(|()| valid));
        let valid = match checked {
            Ok(valid) => valid,
            Err(error) => {
                merezha_telemetry::event!(
                    WARN,
                    "notification.invalid",
                    topic = topic.as_str(),
                    source,
                    notification_id = notification_id.to_string().as_str(),
                    code = error.code()
                );
                core.metrics.request(source, &topic, "invalid");
                return Err(error);
            }
        };
        let payload = ExpandPayload::new(
            source,
            notification_id,
            &request,
            &valid.template,
            &valid.channels,
            core.now(),
        );
        let key = DedupeKey::new(format!("request:{notification_id}"))
            .map_err(|_| NotifyError::Unavailable)?;
        let record = OutboxRecord {
            dedupe_key: key,
            target: OutboxTarget::Job {
                queue: Identity::new(EXPAND_QUEUE).map_err(|_| NotifyError::Unavailable)?,
                kind: Identity::new(EXPAND_KIND).map_err(|_| NotifyError::Unavailable)?,
            },
            payload: payload.to_value(),
            contains_secret: !request.secret_data.is_empty(),
            // Nothing exists before `not_before`: expansion waits for it.
            run_at: request.not_before,
            priority: priority(valid.topic),
            context: MessageContext {
                correlation_id: request.correlation.correlation_id.clone(),
                causation_id: request.correlation.causation_id.clone(),
                tenant: request.tenant.clone(),
            },
        };
        drop(payload);
        let appended = core
            .outbox
            .append(tx, &[record], OnDuplicate::Ignore)
            .await
            .map_err(|failure| {
                if failure.code == "outbox_payload_limit" {
                    NotifyError::DataTooLarge
                } else {
                    NotifyError::Unavailable
                }
            })?;
        let duplicate = appended.duplicates > 0;
        let id = notification_id.to_string();
        if duplicate {
            merezha_telemetry::event!(
                DEBUG,
                "notification.duplicate",
                topic = topic.as_str(),
                source,
                notification_id = id.as_str()
            );
        } else {
            merezha_telemetry::event!(
                DEBUG,
                "notification.requested",
                topic = topic.as_str(),
                source,
                notification_id = id.as_str()
            );
        }
        core.metrics.request(
            source,
            &topic,
            if duplicate { "duplicate" } else { "accepted" },
        );
        Ok(NotificationHandle {
            notification_id,
            duplicate,
        })
    }
    async fn append_webhook(
        &self,
        tx: &mut Tx<'_>,
        event: WebhookEvent,
    ) -> Result<WebhookEventHandle, NotifyError> {
        let core = self.core()?;
        let webhooks = core
            .settings
            .webhooks
            .as_ref()
            .ok_or(NotifyError::WebhooksDisabled)?;
        let event_type = event.event_type.as_str();
        if !webhooks.event_types.contains_key(event_type)
            && !MODULE_EVENT_TYPES.contains(&event_type)
        {
            return Err(NotifyError::UnknownEventType);
        }
        let source = self.source.as_str();
        let id = ids::webhook_event_id(source, event.source_event_id.as_str());
        let mut body = Map::new();
        body.insert("v".into(), Value::from(1));
        body.insert("webhook_event_id".into(), Value::String(id.to_string()));
        body.insert("source".into(), Value::String(source.to_owned()));
        body.insert(
            "source_event_id".into(),
            Value::String(event.source_event_id.as_str().to_owned()),
        );
        body.insert(
            "tenant".into(),
            event
                .tenant
                .as_ref()
                .map_or(Value::Null, |t| Value::String(t.as_str().to_owned())),
        );
        body.insert("type".into(), Value::String(event_type.to_owned()));
        body.insert(
            "occurred_at".into(),
            Value::String(event.occurred_at.to_string()),
        );
        body.insert("data".into(), event.data);
        let payload = Value::Object(body);
        if payload.to_string().len() > webhooks.max_payload_bytes as usize {
            return Err(NotifyError::DataTooLarge);
        }
        let record = OutboxRecord {
            dedupe_key: DedupeKey::new(format!("webhook_event:{id}"))
                .map_err(|_| NotifyError::Unavailable)?,
            target: OutboxTarget::Job {
                queue: Identity::new(EXPAND_QUEUE).map_err(|_| NotifyError::Unavailable)?,
                kind: Identity::new(WEBHOOK_EVENT_KIND).map_err(|_| NotifyError::Unavailable)?,
            },
            payload,
            contains_secret: false,
            run_at: None,
            priority: 0,
            context: MessageContext {
                correlation_id: Identity::new(id.simple().to_string())
                    .map_err(|_| NotifyError::Unavailable)?,
                causation_id: None,
                tenant: event.tenant.clone(),
            },
        };
        let appended = core
            .outbox
            .append(tx, &[record], OnDuplicate::Ignore)
            .await
            .map_err(|failure| {
                if failure.code == "outbox_payload_limit" {
                    NotifyError::DataTooLarge
                } else {
                    NotifyError::Unavailable
                }
            })?;
        Ok(WebhookEventHandle {
            webhook_event_id: id,
            duplicate: appended.duplicates > 0,
        })
    }
}

/// Higher topic priorities run first in the expand and delivery queues.
pub(crate) fn priority(topic: &Topic) -> i16 {
    match topic.priority {
        crate::topics::Priority::High => 10,
        crate::topics::Priority::Normal => 0,
        crate::topics::Priority::Low => -10,
    }
}

impl Notifier for BoundNotifier {
    fn validate(&self, request: &NotificationRequest) -> Result<(), NotifyError> {
        // Validation needs only the configuration, so it works before the
        // block started.
        let checked = match self.slot.get() {
            Some(core) => (core.settings.clone(), core.registry.clone()),
            None => {
                let checked = self.checked.get().ok_or(NotifyError::Unavailable)?;
                (checked.settings.clone(), checked.registry.clone())
            }
        };
        validate(&checked.0, &checked.1, request).map(|_| ())
    }
    fn request<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        request: NotificationRequest,
    ) -> BoxFuture<'a, Result<NotificationHandle, NotifyError>> {
        Box::pin(self.append_request(tx, request))
    }
    fn emit_webhook<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        event: WebhookEvent,
    ) -> BoxFuture<'a, Result<WebhookEventHandle, NotifyError>> {
        Box::pin(self.append_webhook(tx, event))
    }
}
