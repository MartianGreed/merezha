//! The webhook maintenance tasks (NT §2.5, §5.5), on `notify.maintenance`:
//!
//! - [`auto_disable`] (`webhook_auto_disable`, every 5 minutes): an active
//!   endpoint failing since more than `webhooks.auto_disable_after` ago with
//!   at least 10 failed attempts since then is disabled (`failing`), its
//!   queued deliveries are cancelled, an audit row is written, and, in the
//!   same transaction, `notifications.webhook_disabled` is requested for
//!   `webhooks.alert_roles` in the endpoint's tenant, or
//!   `webhooks.application_alert_roles` for an application-level endpoint
//!   (none when the list is empty). The disable is logged
//!   `notification.webhook_endpoint_disabled` either way.
//! - [`retention`] (part of the daily `retention` task): event bodies past
//!   `retention.webhook_deliveries` are nulled (a redelivery then answers
//!   410 `event_body_expired`), events past `retention.deliveries` with
//!   no open delivery are deleted with their deliveries and attempts, audit
//!   rows go after [`AUDIT_RETENTION`], previous secrets whose rotation
//!   window closed are dropped, and, while `secret_key_previous` is set,
//!   secrets only it opens are sealed again under `secret_key`.
use super::{AUTO_DISABLE_FAILURES, secrets::SecretBox, sql, worker};
use crate::notifier::{Core, request_in};
use merezha_domain::{Failure, Identity};
use merezha_platform::ports::{
    ChannelSelection, Correlation, DedupeKey, NotificationRequest, NotifyError, Recipient, Source,
};
use merezha_postgres::classify;
use serde_json::{Map, Value};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

/// The topic of the alert (NT §3.3).
pub const WEBHOOK_DISABLED_TOPIC: &str = "notifications.webhook_disabled";
/// How long audit rows are kept: identity's `audit.retention` default.
pub const AUDIT_RETENTION: Duration = Duration::from_secs(400 * 86_400);
const BATCH: i64 = 500;
const MAX_BATCHES: usize = 20;

fn before(now: jiff::Timestamp, by: Duration) -> Result<jiff::Timestamp, Failure> {
    jiff::SignedDuration::try_from(by)
        .ok()
        .and_then(|d| now.checked_sub(d).ok())
        .ok_or_else(|| Failure::permanent("invalid_config"))
}

/// The alert of a disabled endpoint, or `None` when no role is configured
/// for its registry.
fn alert(
    roles: &[String],
    endpoint: Uuid,
    tenant: Option<&str>,
    disabled_at: jiff::Timestamp,
) -> Result<Option<NotificationRequest>, Failure> {
    if roles.is_empty() {
        return Ok(None);
    }
    let roles = roles
        .iter()
        .map(|role| Identity::new(role.as_str()))
        .collect::<Result<Vec<_>, _>>()?;
    let tenant = tenant.map(Identity::new).transpose()?;
    let recipient = match &tenant {
        Some(id) => Recipient::Tenant {
            id: id.clone(),
            roles: Some(roles),
        },
        None => Recipient::Application { roles },
    };
    let mut data = Map::new();
    data.insert("endpoint_id".into(), Value::String(endpoint.to_string()));
    Ok(Some(NotificationRequest {
        topic: Identity::new(WEBHOOK_DISABLED_TOPIC)?,
        recipients: vec![recipient],
        channels: ChannelSelection::TopicDefault,
        template: None,
        data,
        secret_data: Map::new(),
        locale: None,
        dedupe_key: DedupeKey::new(format!("whdisable:{endpoint}:{}", disabled_at.as_second()))?,
        tenant,
        members: Vec::new(),
        not_before: None,
        expires_at: None,
        collapse_key: None,
        correlation: Correlation {
            correlation_id: Identity::new(endpoint.simple().to_string())?,
            causation_id: None,
        },
    }))
}

/// `webhook_auto_disable`: the endpoints it disabled.
pub async fn auto_disable(core: &Arc<Core>, deadline: Instant) -> Result<u64, Failure> {
    let Some(sender) = core.webhooks.clone() else {
        return Ok(0);
    };
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        let now = core.now();
        let since = before(now, sender.settings.auto_disable_after)?;
        let candidates: Vec<Uuid> = {
            let client = core.database.acquire(deadline).await?;
            core.database
                .query(
                    &client,
                    &sql::AUTO_DISABLE_CANDIDATES,
                    &[&since, &AUTO_DISABLE_FAILURES, &BATCH],
                )
                .await
                .map_err(|e| classify(&e))?
                .iter()
                .map(|row| row.get(0))
                .collect()
        };
        for endpoint in &candidates {
            let endpoint = *endpoint;
            let core = core.clone();
            let settings = sender.settings.clone();
            let db = core.database.clone();
            let cancelled = db
                .clone()
                .transaction(deadline, move |tx| {
                    Box::pin(async move {
                        let Some((tenant, cancelled)) = worker::disable(
                            &db,
                            tx,
                            endpoint,
                            "failing",
                            now,
                            Some(since),
                            worker::SYSTEM_ACTOR,
                        )
                        .await?
                        else {
                            return Ok(None);
                        };
                        let roles = match tenant {
                            Some(_) => &settings.alert_roles,
                            None => &settings.application_alert_roles,
                        };
                        if let Some(request) = alert(roles, endpoint, tenant.as_deref(), now)? {
                            let source = Source::new(crate::NAME)?;
                            match request_in(&core, &source, tx, request).await {
                                Ok(_) => {}
                                // The disable must not wait on the alert's
                                // validity; the refusal is logged
                                // `notification.invalid`.
                                Err(NotifyError::Unavailable) => {
                                    return Err(Failure::transient("unavailable"));
                                }
                                Err(_) => {}
                            }
                        }
                        Ok(Some(cancelled))
                    })
                })
                .await?;
            if let Some(cancelled) = cancelled {
                worker::disabled(&sender, endpoint, "failing", cancelled);
                total += 1;
            }
        }
        if (candidates.len() as i64) < BATCH {
            break;
        }
    }
    Ok(total)
}

/// Run `statement` (`$1` a time, `$2` the batch size) until it changes
/// fewer rows than a batch.
async fn batches(
    core: &Core,
    statement: &merezha_postgres::Statement,
    at: jiff::Timestamp,
    deadline: Instant,
) -> Result<u64, Failure> {
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        let client = core.database.acquire(deadline).await?;
        let changed = core
            .database
            .execute(&client, statement, &[&at, &BATCH])
            .await
            .map_err(|e| classify(&e))?;
        total += changed;
        if changed < BATCH as u64 {
            break;
        }
    }
    Ok(total)
}

/// The webhook part of `retention`: the rows it changed.
pub async fn retention(core: &Arc<Core>, deadline: Instant) -> Result<u64, Failure> {
    let now = core.now();
    let retention = &core.settings.retention;
    let mut total = 0;
    total += batches(
        core,
        &sql::RETENTION_BODIES,
        before(now, retention.webhook_deliveries)?,
        deadline,
    )
    .await?;
    total += batches(
        core,
        &sql::RETENTION_EVENTS,
        before(now, retention.deliveries)?,
        deadline,
    )
    .await?;
    total += batches(
        core,
        &sql::RETENTION_AUDIT,
        before(now, AUDIT_RETENTION)?,
        deadline,
    )
    .await?;
    total += batches(core, &sql::PREVIOUS_EXPIRE, now, deadline).await?;
    let secrets = SecretBox::new(
        &core.settings.secret_key,
        core.settings.secret_key_previous.as_ref(),
    );
    if secrets.has_previous() {
        total += reseal(core, &secrets, deadline).await?;
    }
    Ok(total)
}

/// Seal again under `secret_key` every stored secret only
/// `secret_key_previous` opens. A secret neither key opens is left as it
/// is (its deliveries fail `secret_unreadable`).
async fn reseal(core: &Core, secrets: &SecretBox, deadline: Instant) -> Result<u64, Failure> {
    let mut after = Uuid::nil();
    let mut total = 0;
    for _ in 0..MAX_BATCHES {
        let client = core.database.acquire(deadline).await?;
        let rows = core
            .database
            .query(&client, &sql::SECRETS_PAGE, &[&after, &BATCH])
            .await
            .map_err(|e| classify(&e))?;
        for row in &rows {
            let id: Uuid = row.get(0);
            after = id;
            let (ct, nonce): (Vec<u8>, Vec<u8>) = (row.get(1), row.get(2));
            let previous: (Option<Vec<u8>>, Option<Vec<u8>>) = (row.get(3), row.get(4));
            let Ok((current, old_key)) = secrets.open(id, &ct, &nonce) else {
                continue;
            };
            let prior = match &previous {
                (Some(pct), Some(pnonce)) => secrets.open(id, pct, pnonce).ok(),
                _ => None,
            };
            if !old_key && !prior.as_ref().is_some_and(|(_, old)| *old) {
                continue;
            }
            let sealed = secrets.seal(id, &current, core.random.as_ref())?;
            let (prior_ct, prior_nonce) = match &prior {
                Some((secret, _)) => {
                    let sealed = secrets.seal(id, secret, core.random.as_ref())?;
                    (Some(sealed.ciphertext), Some(sealed.nonce))
                }
                // A previous secret no key opens is dropped with the reseal.
                None => (None, None),
            };
            total += core
                .database
                .execute(
                    &client,
                    &sql::SECRETS_RESEAL,
                    &[
                        &id,
                        &sealed.ciphertext,
                        &sealed.nonce,
                        &prior_ct,
                        &prior_nonce,
                        &ct,
                    ],
                )
                .await
                .map_err(|e| classify(&e))?;
        }
        if (rows.len() as i64) < BATCH {
            break;
        }
    }
    Ok(total)
}
