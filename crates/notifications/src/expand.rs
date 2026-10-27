//! The expansion handler (NT §2.1 to §2.5, §3.2, §5.4, §6): one
//! `notify.expand` job turns a request into the notification row, its
//! deliveries, the inbox items and the delivery job records.
//!
//! Recipients are resolved through the Directory, and preferences,
//! suppressions and time zones read, before the transaction opens (R4).
//! The transaction then writes everything or nothing (invariant 3): the
//! notification (`ON CONFLICT DO NOTHING`, so a second run of the same job
//! writes nothing more), the deliveries, the inbox items numbered from the
//! users' counters locked in ascending user order, the `pg_notify` wake-ups
//! and the `OutboxTarget::Job` records of the email, SMS and push
//! deliveries (R1), at most 256 per append.
//!
//! A topic with `max_per_target_per_hour` (NT §6, NT-N8) caps each
//! target: inside the transaction, under a lock per `(topic, channel,
//! target key)`, a target that already got that many of the topic's
//! deliveries in the last hour is `skipped` / `target_rate_limited`.
//!
//! A push target is one subscription (NT-N6): a user gets one push delivery
//! per live subscription made with the current VAPID key and in the
//! notification's tenant scope, or one `skipped` / `no_subscription`
//! delivery without any.
use crate::{
    faults,
    handler::Outcome,
    ids,
    notifier::{Core, priority},
    payload::{DeliverPayload, ExpandPayload, StoredRecipient},
    policy::{self, UserPreferences, reason},
    sql,
    templates::{RenderContext, Rendered},
    topics::{self, Category, Topic},
};
use merezha_domain::Identity;
use merezha_platform::ports::{
    Channel, Contact, DedupeKey, MAX_PAGE, MAX_RECORDS_PER_APPEND, MAX_USERS_PER_CALL,
    MessageContext, OnDuplicate, OutboxRecord, OutboxTarget, Page, PageRequest,
};
use merezha_postgres::classify;
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Instant,
};
use tracing::Instrument;
use uuid::Uuid;

/// The job kind and queue of each delivering channel (NT §2.5).
pub fn delivery_queue(channel: Channel) -> Option<(&'static str, &'static str)> {
    match channel {
        Channel::Email => Some(("notify.email", "notify.deliver.email")),
        Channel::Sms => Some(("notify.sms", "notify.deliver.sms")),
        Channel::Push => Some(("notify.push", "notify.deliver.push")),
        Channel::Inbox => None,
    }
}

/// A suppression's topic and reason.
type Suppression = (Option<String>, String);
/// A rendered inbox item: title, body, link and the payload as JSON text.
type InboxContent = (String, String, Option<String>, String);

/// A resolved recipient: a Directory contact, an explicit address, or one
/// that could not be resolved (its placeholder key and reason).
#[derive(Clone)]
struct Person {
    user: Option<String>,
    email: Option<String>,
    phone: Option<String>,
    name: Option<String>,
    locales: Vec<String>,
    /// A contact (inbox and push go to its user), not an explicit address.
    contact: bool,
    unresolved: Option<(String, &'static str)>,
}

/// One delivery to write.
#[derive(Clone)]
struct Target {
    id: Uuid,
    channel: Channel,
    key: String,
    user: Option<String>,
    address: Option<String>,
    name: Option<String>,
    locale: String,
    status: &'static str,
    reason: Option<&'static str>,
    provider: Option<&'static str>,
    inbox: Option<InboxContent>,
    /// The push subscription of a push target.
    subscription: Option<Uuid>,
}

/// What a failed resolution means for the job.
enum Resolve {
    Ok(Vec<Person>, bool),
    Retry(String),
}

/// Run one expansion.
pub(crate) async fn run(core: &Core, deadline: Instant, payload: ExpandPayload) -> Outcome {
    let span = tracing::info_span!(
        target: merezha_telemetry::TARGET,
        "notify expand",
        otel.name = "notify expand",
        notify.topic = payload.topic.as_str(),
        notify.source = payload.source.as_str(),
        notify.notification_id = payload.notification_id.to_string().as_str(),
        notify.target_count = tracing::field::Empty,
    );
    expand(core, deadline, payload).instrument(span).await
}

async fn expand(core: &Core, deadline: Instant, payload: ExpandPayload) -> Outcome {
    if payload.v != ExpandPayload::VERSION {
        return Outcome::Permanent("invalid_job_payload".into());
    }
    match exists(core, deadline, payload.notification_id).await {
        Ok(true) => return Outcome::Done,
        Ok(false) => {}
        Err(failure) => return Outcome::retry(&failure.code),
    }
    let Some(topic) = core.topic(&payload.topic).cloned() else {
        return Outcome::Permanent("unknown_topic".into());
    };
    let channels: Vec<Channel> = payload
        .channels
        .iter()
        .filter_map(|c| topics::channel(c))
        .collect();
    let max_targets = core.settings.limits.max_targets;
    let (people, overflow) = match resolve(core, deadline, &payload, max_targets).await {
        Resolve::Ok(people, overflow) => (people, overflow),
        Resolve::Retry(code) => return Outcome::retry(&code),
    };
    let subscriptions = match push_subscriptions(core, deadline, &payload, &channels, &people).await
    {
        Ok(found) => found,
        Err(failure) => return Outcome::retry(&failure.code),
    };
    let mut targets = targets(core, &payload, &channels, &people, &subscriptions);
    let cap = match topic.category {
        Category::Marketing => core.settings.limits.max_marketing_targets.min(max_targets),
        _ => max_targets,
    };
    let rejected = overflow || targets.len() > cap;
    tracing::Span::current().record("notify.target_count", targets.len() as u64);
    let now = core.now();
    if !rejected
        && let Err(failure) = decide(core, deadline, &payload, &topic, &mut targets, now).await
    {
        return Outcome::retry(&failure.code);
    }
    let written = write(core, deadline, &payload, &topic, targets, rejected, now).await;
    match written {
        Ok(None) => Outcome::Done,
        Ok(Some(targets)) => {
            report(core, &payload, &topic, &targets, rejected);
            core.faults.hit(faults::AFTER_EXPANSION_COMMIT).await;
            Outcome::Done
        }
        Err(failure) => Outcome::retry(&failure.code),
    }
}

async fn exists(core: &Core, deadline: Instant, id: Uuid) -> Result<bool, merezha_domain::Failure> {
    let client = core.database.acquire(deadline).await?;
    let row = core
        .database
        .query_opt(&client, &sql::NOTIFICATION_EXISTS, &[&id])
        .await
        .map_err(|e| classify(&e))?;
    Ok(row.is_some())
}

/// A Directory call bounded by the job deadline.
async fn bounded<T>(
    deadline: Instant,
    call: impl Future<Output = Result<T, merezha_domain::Failure>>,
) -> Result<T, String> {
    match tokio::time::timeout_at(deadline.into(), call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(failure)) => Err(failure.code),
        Err(_) => Err("directory_timeout".into()),
    }
}

fn identity(text: &str) -> Option<Identity> {
    Identity::new(text).ok()
}

fn from_contact(contact: &Contact) -> Person {
    Person {
        user: Some(contact.user.as_str().to_owned()),
        email: Some(contact.email.clone()),
        phone: None,
        name: contact.display_name.clone(),
        locales: contact.locale.iter().cloned().collect(),
        contact: true,
        unresolved: None,
    }
}
fn unresolved(key: String, why: &'static str, user: Option<String>) -> Person {
    Person {
        user,
        email: None,
        phone: None,
        name: None,
        locales: Vec::new(),
        contact: true,
        unresolved: Some((key, why)),
    }
}

/// Every recipient as people, Directory lookups included, and whether a
/// group had more members than `max_targets`.
async fn resolve(
    core: &Core,
    deadline: Instant,
    payload: &ExpandPayload,
    max_targets: usize,
) -> Resolve {
    let directory = core.directory.as_ref();
    let resolves = directory.resolves();
    let mut people = Vec::new();
    let mut overflow = false;
    // Explicit users, looked up in batches.
    let users: Vec<Identity> = payload
        .recipients
        .iter()
        .filter_map(|r| match r {
            StoredRecipient::User { id } => identity(id),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut contacts: HashMap<String, Contact> = HashMap::new();
    if resolves {
        for chunk in users.chunks(MAX_USERS_PER_CALL) {
            match bounded(deadline, directory.users(chunk)).await {
                Ok(found) => {
                    for contact in found {
                        contacts.insert(contact.user.as_str().to_owned(), contact);
                    }
                }
                Err(code) => return Resolve::Retry(code),
            }
        }
    }
    for recipient in &payload.recipients {
        match recipient {
            StoredRecipient::User { id } => match contacts.get(id) {
                Some(contact) => people.push(from_contact(contact)),
                None if resolves => people.push(unresolved(
                    ids::user_key(id),
                    reason::UNKNOWN_USER,
                    Some(id.clone()),
                )),
                None => people.push(unresolved(
                    ids::user_key(id),
                    reason::NO_DIRECTORY,
                    Some(id.clone()),
                )),
            },
            StoredRecipient::Email {
                address,
                name,
                user,
                locale,
            } => people.push(Person {
                user: user.clone(),
                email: Some(address.clone()),
                phone: None,
                name: name.clone(),
                locales: locale.iter().cloned().collect(),
                contact: false,
                unresolved: None,
            }),
            StoredRecipient::Phone { e164, user, locale } => people.push(Person {
                user: user.clone(),
                email: None,
                phone: Some(e164.clone()),
                name: None,
                locales: locale.iter().cloned().collect(),
                contact: false,
                unresolved: None,
            }),
            StoredRecipient::Tenant { id, roles } => {
                if !resolves {
                    people.push(unresolved(
                        ids::group_key("tenant", id),
                        reason::NO_DIRECTORY,
                        None,
                    ));
                    continue;
                }
                let Some(tenant) = identity(id) else {
                    continue;
                };
                let roles: Option<Vec<Identity>> = roles
                    .as_ref()
                    .map(|r| r.iter().filter_map(|r| identity(r)).collect());
                match page_all(deadline, max_targets, |page| {
                    directory.tenant_members(&tenant, roles.as_deref(), page)
                })
                .await
                {
                    Ok((members, more)) => {
                        overflow |= more;
                        people.extend(members.iter().map(from_contact));
                    }
                    Err(code) => return Resolve::Retry(code),
                }
            }
            StoredRecipient::Application { roles } => {
                if !resolves {
                    people.push(unresolved(
                        ids::group_key("application", &roles.join(",")),
                        reason::NO_DIRECTORY,
                        None,
                    ));
                    continue;
                }
                let roles: Vec<Identity> = roles.iter().filter_map(|r| identity(r)).collect();
                match page_all(deadline, max_targets, |page| {
                    directory.application_members(Some(&roles), page)
                })
                .await
                {
                    Ok((members, more)) => {
                        overflow |= more;
                        people.extend(members.iter().map(from_contact));
                    }
                    Err(code) => return Resolve::Retry(code),
                }
            }
        }
        if people.len() > max_targets {
            overflow = true;
            break;
        }
    }
    Resolve::Ok(people, overflow)
}

/// Page through a member list up to `max + 1` contacts, to detect overflow.
async fn page_all<'a, F>(
    deadline: Instant,
    max: usize,
    mut fetch: F,
) -> Result<(Vec<Contact>, bool), String>
where
    F: FnMut(
        PageRequest,
    )
        -> merezha_platform::BoxFuture<'a, Result<Page<Contact>, merezha_domain::Failure>>,
{
    let mut members = Vec::new();
    let mut after = None;
    loop {
        let wanted = (max + 1)
            .saturating_sub(members.len())
            .clamp(1, usize::from(MAX_PAGE));
        let page = PageRequest::new(after.clone(), wanted as u16).map_err(|f| f.code)?;
        let page = bounded(deadline, fetch(page)).await?;
        members.extend(page.items);
        if members.len() > max {
            members.truncate(max + 1);
            return Ok((members, true));
        }
        match page.next {
            Some(next) => after = Some(next),
            None => return Ok((members, false)),
        }
    }
}

/// The push subscriptions of the resolved users that this notification may
/// reach, by user (NT §5.3): made with the current VAPID key, not expired,
/// and in the notification's tenant scope. At most `max_targets + 1` are
/// read, so a larger set is rejected as too many targets.
async fn push_subscriptions(
    core: &Core,
    deadline: Instant,
    payload: &ExpandPayload,
    channels: &[Channel],
    people: &[Person],
) -> Result<HashMap<String, Vec<Uuid>>, merezha_domain::Failure> {
    let mut found: HashMap<String, Vec<Uuid>> = HashMap::new();
    let Some(sender) = &core.push else {
        return Ok(found);
    };
    if !channels.contains(&Channel::Push) {
        return Ok(found);
    }
    let users: Vec<String> = people
        .iter()
        .filter(|p| p.contact && p.unresolved.is_none())
        .filter_map(|p| p.user.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if users.is_empty() {
        return Ok(found);
    }
    let limit = i64::try_from(core.settings.limits.max_targets).unwrap_or(i64::MAX - 1) + 1;
    let client = core.database.acquire(deadline).await?;
    let rows = core
        .database
        .query(
            &client,
            &crate::push::sql::TARGETS,
            &[
                &users,
                &sender.vapid().key_id(),
                &payload.tenant,
                &core.now(),
                &limit,
            ],
        )
        .await
        .map_err(|e| classify(&e))?;
    for row in rows {
        found.entry(row.get(0)).or_default().push(row.get(1));
    }
    Ok(found)
}

/// People times channels, one target per `(channel, target key)`.
fn targets(
    core: &Core,
    payload: &ExpandPayload,
    channels: &[Channel],
    people: &[Person],
    subscriptions: &HashMap<String, Vec<Uuid>>,
) -> Vec<Target> {
    let mut out: Vec<Target> = Vec::new();
    let mut seen: HashMap<(Channel, String), usize> = HashMap::new();
    for person in people {
        let mut candidates: Vec<&str> = Vec::new();
        if let Some(locale) = &payload.locale {
            candidates.push(locale);
        }
        candidates.extend(person.locales.iter().map(String::as_str));
        let locale = core.registry.resolve_locale(candidates);
        for channel in channels {
            type Candidate = (String, Option<String>, Option<(&'static str, &'static str)>);
            let mut found: Vec<(Candidate, Option<Uuid>)> = Vec::new();
            match (&person.unresolved, channel) {
                (Some((key, why)), _) => {
                    found.push(((key.clone(), None, Some(("skipped", *why))), None))
                }
                (None, Channel::Email) => match &person.email {
                    Some(email) => {
                        found.push(((ids::email_key(email), Some(email.clone()), None), None))
                    }
                    None => continue,
                },
                (None, Channel::Sms) => match &person.phone {
                    Some(phone) => {
                        found.push(((ids::sms_key(phone), Some(phone.clone()), None), None))
                    }
                    None => continue,
                },
                (None, Channel::Inbox) => match (&person.user, person.contact) {
                    (Some(user), true) => found.push(((ids::inbox_key(user), None, None), None)),
                    _ => continue,
                },
                (None, Channel::Push) => match (&person.user, person.contact) {
                    // One target per subscription (NT-N6).
                    (Some(user), true) => match subscriptions.get(user) {
                        Some(owned) if !owned.is_empty() => {
                            for subscription in owned {
                                found.push((
                                    (ids::push_subscription_key(*subscription), None, None),
                                    Some(*subscription),
                                ));
                            }
                        }
                        // Without a push section the channel is disabled,
                        // which `decide` records.
                        _ if core.push.is_none() => {
                            found.push(((ids::push_key(user), None, None), None));
                        }
                        _ => found.push((
                            (
                                ids::push_key(user),
                                None,
                                Some(("skipped", reason::NO_SUBSCRIPTION)),
                            ),
                            None,
                        )),
                    },
                    _ => continue,
                },
            }
            for ((key, address, status), subscription) in found {
                if key.len() > ids::MAX_TARGET_KEY {
                    continue;
                }
                if let Some(&index) = seen.get(&(*channel, key.clone())) {
                    // The same mailbox from a user and an address is one
                    // delivery; keep what the first lacked.
                    let existing = &mut out[index];
                    if existing.user.is_none() {
                        existing.user = person.user.clone();
                    }
                    if existing.name.is_none() {
                        existing.name = person.name.clone();
                    }
                    continue;
                }
                seen.insert((*channel, key.clone()), out.len());
                let (status, reason) = match status {
                    Some((status, reason)) => (status, Some(reason)),
                    None => ("queued", None),
                };
                out.push(Target {
                    id: ids::delivery_id(payload.notification_id, *channel, &key),
                    channel: *channel,
                    key,
                    user: person.user.clone(),
                    address,
                    name: person.name.clone(),
                    locale: locale.clone(),
                    status,
                    reason,
                    provider: None,
                    inbox: None,
                    subscription,
                });
            }
        }
    }
    out
}

/// The status of every target still queued: channel availability,
/// expiry, preferences, suppressions; inbox items are rendered here.
async fn decide(
    core: &Core,
    deadline: Instant,
    payload: &ExpandPayload,
    topic: &Topic,
    targets: &mut [Target],
    now: jiff::Timestamp,
) -> Result<(), merezha_domain::Failure> {
    let users: Vec<String> = targets
        .iter()
        .filter_map(|t| t.user.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let addresses: Vec<String> = targets
        .iter()
        .filter(|t| matches!(t.channel, Channel::Email | Channel::Sms))
        .filter_map(|t| t.address.as_ref().map(|a| a.to_lowercase()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let client = core.database.acquire(deadline).await?;
    let db = &core.database;
    let mut prefs: HashMap<(String, String), UserPreferences> = HashMap::new();
    let mut zones: HashMap<String, String> = HashMap::new();
    if !users.is_empty() {
        let rows = db
            .query(&client, &sql::PREFERENCES_FOR_USERS, &[&users, &topic.name])
            .await
            .map_err(|e| classify(&e))?;
        for row in rows {
            let (user, row_topic, channel, enabled): (String, String, String, bool) =
                (row.get(0), row.get(1), row.get(2), row.get(3));
            let entry = prefs.entry((user, channel)).or_default();
            if row_topic == "*" {
                entry.any = Some(enabled);
            } else {
                entry.topic = Some(enabled);
            }
        }
        let rows = db
            .query(&client, &sql::TIMEZONES_FOR_USERS, &[&users])
            .await
            .map_err(|e| classify(&e))?;
        for row in rows {
            zones.insert(row.get(0), row.get(1));
        }
    }
    let mut suppressions: HashMap<(String, String), Vec<Suppression>> = HashMap::new();
    if !addresses.is_empty() {
        let rows = db
            .query(
                &client,
                &sql::SUPPRESSIONS_FOR_ADDRESSES,
                &[&addresses, &topic.name],
            )
            .await
            .map_err(|e| classify(&e))?;
        for row in rows {
            suppressions
                .entry((row.get(0), row.get(1)))
                .or_default()
                .push((row.get(2), row.get(3)));
        }
    }
    drop(client);
    let expired = payload.expires_at.is_some_and(|at| at <= now);
    let empty = Map::new();
    for target in targets.iter_mut().filter(|t| t.status == "queued") {
        let skip = |reason| ("skipped", Some(reason));
        let decided = if !core.settings.channel_available(target.channel) {
            Some(skip(reason::CHANNEL_DISABLED))
        } else if expired {
            Some(("expired", Some(reason::EXPIRED)))
        } else {
            None
        };
        let decided = decided.or_else(|| {
            let user = target.user.as_ref()?;
            let found = prefs
                .get(&(user.clone(), target.channel.as_str().to_owned()))
                .copied()
                .unwrap_or_default();
            policy::user_allows(topic, target.channel, found)
                .err()
                .map(skip)
        });
        let decided = decided.or_else(|| {
            let address = target.address.as_ref()?.to_lowercase();
            let rows = suppressions.get(&(target.channel.as_str().to_owned(), address))?;
            rows.iter()
                .any(|(t, r)| {
                    policy::suppression_blocks(r, t.as_deref(), &topic.name, topic.category)
                })
                .then(|| skip(reason::SUPPRESSED))
        });
        if let Some((status, reason)) = decided {
            target.status = status;
            target.reason = reason;
            continue;
        }
        if target.channel == Channel::Inbox {
            let timezone = target.user.as_ref().and_then(|u| zones.get(u));
            let context = RenderContext {
                topic: &topic.name,
                category: topic.category,
                notification_id: payload.notification_id,
                data: &payload.data,
                // The inbox never sees secret data (R2).
                secret: &empty,
                recipient_name: target.name.as_deref(),
                timezone: timezone.map(String::as_str),
                unsubscribe: None,
            };
            match core
                .registry
                .render(&payload.template, Channel::Inbox, &target.locale, &context)
            {
                Ok(Rendered::Inbox(inbox)) => {
                    target.status = "delivered";
                    target.provider = Some("inbox");
                    target.inbox = Some((
                        inbox.title,
                        inbox.body,
                        inbox.link,
                        Value::Object(inbox.payload).to_string(),
                    ));
                }
                _ => {
                    core.metrics.render_failure(Channel::Inbox, &topic.name);
                    target.status = "failed";
                    target.reason = Some(reason::RENDER_FAILED);
                }
            }
        }
    }
    Ok(())
}

/// The delivery job records of the queued targets.
fn job_records(
    payload: &ExpandPayload,
    topic: &Topic,
    targets: &[Target],
) -> Result<Vec<OutboxRecord>, merezha_domain::Failure> {
    let Some(correlation) = identity(&payload.correlation_id) else {
        return Err(merezha_domain::Failure::permanent("invalid_job_payload"));
    };
    let causation = payload.causation_id.as_deref().and_then(identity);
    let tenant = payload.tenant.as_deref().and_then(identity);
    let has_secret = payload.secret_data.is_some();
    let mut records = Vec::new();
    for target in targets.iter().filter(|t| t.status == "queued") {
        let Some((queue, kind)) = delivery_queue(target.channel) else {
            continue;
        };
        let job = DeliverPayload {
            delivery_id: target.id,
            generation: 0,
            secret_data: payload.secret_data.clone(),
        };
        records.push(OutboxRecord {
            dedupe_key: DedupeKey::new(format!("deliver:{}:g0", target.id))?,
            target: OutboxTarget::Job {
                queue: Identity::new(queue)?,
                kind: Identity::new(kind)?,
            },
            payload: job.to_value(),
            contains_secret: has_secret,
            run_at: None,
            priority: priority(topic),
            context: MessageContext {
                correlation_id: correlation.clone(),
                causation_id: causation.clone(),
                tenant: tenant.clone(),
            },
        });
    }
    Ok(records)
}

/// The targets a topic's `max_per_target_per_hour` counts: those that will
/// be sent (queued) or were just delivered (inbox).
fn counted(target: &Target) -> bool {
    matches!(target.status, "queued" | "delivered")
}

/// Apply `cap`, the topic's `max_per_target_per_hour` (NT §6), in the
/// expansion transaction: lock every counted target's `(topic, channel,
/// target key)` in ascending key order, so concurrent expansions for one
/// target count one after the other, then skip (`target_rate_limited`)
/// each target that already has `cap` deliveries of the topic created in
/// the last hour that were not skipped.
async fn limit(
    db: &merezha_postgres::Database,
    tx: &mut merezha_postgres::Tx<'_>,
    topic: &str,
    cap: u32,
    targets: &mut [Target],
    now: jiff::Timestamp,
) -> Result<(), merezha_domain::Failure> {
    let (channels, keys): (Vec<String>, Vec<String>) = targets
        .iter()
        .filter(|t| counted(t))
        .map(|t| (t.channel.as_str().to_owned(), t.key.clone()))
        .unzip();
    if keys.is_empty() {
        return Ok(());
    }
    db.execute(
        tx,
        &sql::TARGET_LOCKS,
        &[&sql::TARGET_LOCK_CLASS, &topic, &channels, &keys],
    )
    .await
    .map_err(|e| classify(&e))?;
    let since = now
        .checked_sub(jiff::SignedDuration::from_hours(1))
        .unwrap_or(now);
    let rows = db
        .query(tx, &sql::TARGET_RECENT, &[&channels, &keys, &since, &topic])
        .await
        .map_err(|e| classify(&e))?;
    let sent: HashMap<(String, String), i64> = rows
        .iter()
        .map(|row| ((row.get(0), row.get(1)), row.get(2)))
        .collect();
    for target in targets.iter_mut().filter(|t| counted(t)) {
        let recent = sent
            .get(&(target.channel.as_str().to_owned(), target.key.clone()))
            .copied()
            .unwrap_or(0);
        if recent >= i64::from(cap) {
            target.status = "skipped";
            target.reason = Some(reason::TARGET_RATE_LIMITED);
            target.provider = None;
            target.inbox = None;
        }
    }
    Ok(())
}

/// The transaction, which returns the targets as written (the rate cap may
/// skip some). `Ok(None)`: the notification existed, nothing written.
async fn write(
    core: &Core,
    deadline: Instant,
    payload: &ExpandPayload,
    topic: &Topic,
    targets: Vec<Target>,
    rejected: bool,
    now: jiff::Timestamp,
) -> Result<Option<Vec<Target>>, merezha_domain::Failure> {
    let db = core.database.clone();
    let has_secret = payload.secret_data.is_some();
    let (status, status_reason) = if rejected {
        ("rejected", Some(reason::TOO_MANY_TARGETS))
    } else {
        ("expanded", None)
    };
    let target_count = i32::try_from(targets.len()).unwrap_or(i32::MAX);
    let data = Value::Object(payload.data.clone());
    let channels = payload.channels.clone();
    let payload = payload.clone();
    let outbox = core.outbox.clone();
    let expires = now
        .checked_add(
            jiff::SignedDuration::try_from(core.settings.inbox.retention).unwrap_or_default(),
        )
        .unwrap_or(now);
    let topic = topic.clone();
    let id = payload.notification_id;
    db.clone()
        .transaction(deadline, move |tx| {
            Box::pin(async move {
                let mut targets = targets;
                let topic_name = topic.name.clone();
                let category = topic.category.as_str();
                let priority_name = topic.priority.as_str();
                if !rejected && let Some(cap) = topic.max_per_target_per_hour {
                    limit(&db, tx, &topic_name, cap, &mut targets, now).await?;
                }
                let open = targets.iter().any(|t| t.status == "queued");
                let settled = (rejected || !open).then_some(now);
                let records = if rejected {
                    Vec::new()
                } else {
                    job_records(&payload, &topic, &targets)?
                };
                // Inbox items: one per user here, numbered in ascending
                // user order.
                let mut inbox_users: BTreeMap<String, (Uuid, InboxContent)> = BTreeMap::new();
                for target in &targets {
                    if let (Some(inbox), Some(user)) = (&target.inbox, &target.user) {
                        inbox_users.insert(user.clone(), (target.id, inbox.clone()));
                    }
                }
                let columns = Columns::of(&targets);
                let inserted = db
                    .query_opt(
                        tx,
                        &sql::NOTIFICATION_INSERT,
                        &[
                            &id,
                            &payload.source,
                            &payload.dedupe_key,
                            &topic_name,
                            &payload.template,
                            &payload.locale,
                            &payload.tenant,
                            &category,
                            &priority_name,
                            &data,
                            &has_secret,
                            &channels,
                            &payload.collapse_key,
                            &payload.not_before,
                            &payload.expires_at,
                            &payload.correlation_id,
                            &payload.causation_id,
                            &status,
                            &status_reason,
                            &target_count,
                            &payload.requested_at,
                            &now,
                            &settled,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                if inserted.is_none() {
                    return Ok(None);
                }
                if rejected {
                    return Ok(Some(targets));
                }
                let run_at = payload.not_before.unwrap_or(now);
                db.execute(
                    tx,
                    &sql::DELIVERIES_INSERT,
                    &[
                        &id,
                        &run_at,
                        &now,
                        &columns.ids,
                        &columns.channels,
                        &columns.keys,
                        &columns.users,
                        &columns.addresses,
                        &columns.names,
                        &columns.locales,
                        &columns.statuses,
                        &columns.reasons,
                        &columns.providers,
                        &columns.subscriptions,
                    ],
                )
                .await
                .map_err(|e| classify(&e))?;
                if !inbox_users.is_empty() {
                    let users: Vec<String> = inbox_users.keys().cloned().collect();
                    let counts: Vec<i64> = vec![1; users.len()];
                    let rows = db
                        .query(tx, &sql::COUNTERS_TAKE, &[&users, &counts])
                        .await
                        .map_err(|e| classify(&e))?;
                    let seqs: HashMap<String, i64> =
                        rows.iter().map(|r| (r.get(0), r.get(1))).collect();
                    let mut item = InboxColumns::default();
                    for (user, (delivery, (title, body, link, json))) in &inbox_users {
                        let Some(seq) = seqs.get(user) else {
                            continue;
                        };
                        item.ids.push(*delivery);
                        item.users.push(user.clone());
                        item.titles.push(title.clone());
                        item.bodies.push(body.clone());
                        item.links.push(link.clone());
                        item.payloads.push(json.clone());
                        item.seqs.push(*seq);
                    }
                    db.execute(
                        tx,
                        &sql::INBOX_INSERT,
                        &[
                            &payload.tenant,
                            &id,
                            &topic_name,
                            &now,
                            &expires,
                            &item.ids,
                            &item.users,
                            &item.titles,
                            &item.bodies,
                            &item.links,
                            &item.payloads,
                            &item.seqs,
                        ],
                    )
                    .await
                    .map_err(|e| classify(&e))?;
                    db.query(tx, &sql::INBOX_NOTIFY, &[&users])
                        .await
                        .map_err(|e| classify(&e))?;
                }
                for chunk in records.chunks(MAX_RECORDS_PER_APPEND) {
                    outbox.append(tx, chunk, OnDuplicate::Ignore).await?;
                }
                Ok(Some(targets))
            })
        })
        .await
}

/// The delivery columns as parallel arrays.
struct Columns {
    ids: Vec<Uuid>,
    channels: Vec<String>,
    keys: Vec<String>,
    users: Vec<Option<String>>,
    addresses: Vec<Option<String>>,
    names: Vec<Option<String>>,
    locales: Vec<String>,
    statuses: Vec<String>,
    reasons: Vec<Option<String>>,
    providers: Vec<Option<String>>,
    subscriptions: Vec<Option<Uuid>>,
}
impl Columns {
    fn of(targets: &[Target]) -> Self {
        Self {
            ids: targets.iter().map(|t| t.id).collect(),
            channels: targets
                .iter()
                .map(|t| t.channel.as_str().to_owned())
                .collect(),
            keys: targets.iter().map(|t| t.key.clone()).collect(),
            users: targets.iter().map(|t| t.user.clone()).collect(),
            addresses: targets.iter().map(|t| t.address.clone()).collect(),
            names: targets
                .iter()
                .map(|t| t.name.as_ref().map(|n| n.chars().take(200).collect()))
                .collect(),
            locales: targets.iter().map(|t| t.locale.clone()).collect(),
            statuses: targets.iter().map(|t| t.status.to_owned()).collect(),
            reasons: targets
                .iter()
                .map(|t| t.reason.map(str::to_owned))
                .collect(),
            providers: targets
                .iter()
                .map(|t| t.provider.map(str::to_owned))
                .collect(),
            subscriptions: targets.iter().map(|t| t.subscription).collect(),
        }
    }
}
#[derive(Default)]
struct InboxColumns {
    ids: Vec<Uuid>,
    users: Vec<String>,
    titles: Vec<String>,
    bodies: Vec<String>,
    links: Vec<Option<String>>,
    payloads: Vec<String>,
    seqs: Vec<i64>,
}

/// Logs and metrics after commit: ids and codes only.
fn report(core: &Core, payload: &ExpandPayload, topic: &Topic, targets: &[Target], rejected: bool) {
    let id = payload.notification_id.to_string();
    if rejected {
        merezha_telemetry::event!(
            WARN,
            "notification.rejected",
            topic = topic.name.as_str(),
            source = payload.source.as_str(),
            notification_id = id.as_str(),
            code = reason::TOO_MANY_TARGETS
        );
        return;
    }
    for target in targets {
        let delivery = target.id.to_string();
        let why = target.reason.unwrap_or("");
        match target.status {
            "skipped" => merezha_telemetry::event!(
                DEBUG,
                "notification.skipped",
                channel = target.channel.as_str(),
                topic = topic.name.as_str(),
                delivery_id = delivery.as_str(),
                reason = why
            ),
            "expired" => merezha_telemetry::event!(
                WARN,
                "notification.expired",
                channel = target.channel.as_str(),
                topic = topic.name.as_str(),
                delivery_id = delivery.as_str(),
                reason = why
            ),
            "failed" => merezha_telemetry::event!(
                WARN,
                "notification.failed",
                channel = target.channel.as_str(),
                topic = topic.name.as_str(),
                delivery_id = delivery.as_str(),
                code = why
            ),
            _ => {}
        }
        let status: &'static str = match target.status {
            "skipped" => "skipped",
            "expired" => "expired",
            "failed" => "failed",
            "delivered" => "delivered",
            _ => continue,
        };
        core.metrics
            .delivery(target.channel, &topic.name, status, why);
    }
}
