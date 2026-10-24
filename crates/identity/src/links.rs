//! Link requests (section 7, enumeration): password reset, verification
//! resend and magic-link requests do the same work on the request path for
//! every address: validate, charge the rate limits, append one
//! `identity.link_request` job record to the outbox, answer `202`. The job
//! looks the account up, issues the token and requests the notification, so
//! neither the answer nor its timing tells known and unknown addresses
//! apart. The record carries no secret (`contains_secret = false`): the
//! address, the `return_to` path, the digest of the browser's binding
//! cookie, the request id and the client address for the audit row.
use crate::{
    config::Registration,
    core::{Actor, ApiError, Audit, Bound, Client, Core, Purpose, topic},
    normalize::{self, Email},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    BoxFuture,
    ports::{
        DedupeKey, JobContext, JobFailure, JobHandler, MessageContext, OnDuplicate, OutboxRecord,
        OutboxTarget,
    },
};
use serde_json::{Value, json};
use std::sync::{Arc, OnceLock};

/// The job kind; it runs on the cleanup job's queue.
pub const KIND: &str = "identity.link_request";

/// What a link request asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkPurpose {
    Verification,
    MagicLink,
    PasswordReset,
}
impl LinkPurpose {
    fn as_str(self) -> &'static str {
        match self {
            LinkPurpose::Verification => "email_verification",
            LinkPurpose::MagicLink => "magic_link",
            LinkPurpose::PasswordReset => "password_reset",
        }
    }
    fn parse(text: &str) -> Option<Self> {
        match text {
            "email_verification" => Some(LinkPurpose::Verification),
            "magic_link" => Some(LinkPurpose::MagicLink),
            "password_reset" => Some(LinkPurpose::PasswordReset),
            _ => None,
        }
    }
}

/// A queued request, as the job reads it back.
#[derive(Clone)]
pub(crate) struct LinkRequest {
    purpose: LinkPurpose,
    email: Email,
    return_to: Option<String>,
    browser: Option<[u8; 32]>,
    client: Client,
}

impl Core {
    /// The request path: one transaction appending the job record.
    pub(crate) async fn request_link(
        self: &Arc<Self>,
        purpose: LinkPurpose,
        client: &Client,
        email: &Email,
        return_to: Option<&str>,
        browser: Option<[u8; 32]>,
    ) -> Result<(), ApiError> {
        let payload = json!({
            "purpose": purpose.as_str(),
            "email": email.display(),
            "return_to": return_to,
            "browser": browser.map(|b| URL_SAFE_NO_PAD.encode(b)),
            "request_id": client.request_id,
            "ip": client.ip.map(|ip| ip.to_string()),
            "user_agent": client.user_agent,
        });
        let record = OutboxRecord {
            dedupe_key: DedupeKey::new(format!("link:{}", self.new_id()?.simple()))?,
            target: OutboxTarget::Job {
                queue: Identity::new(crate::cleanup::QUEUE)?,
                kind: Identity::new(KIND)?,
            },
            payload,
            contains_secret: false,
            run_at: None,
            priority: 0,
            context: MessageContext {
                correlation_id: client.correlation(self.ctx.random()),
                causation_id: None,
                tenant: None,
            },
        };
        let core = self.clone();
        self.in_tx(move |tx| {
            Box::pin(async move {
                core.outbox
                    .append(tx, &[record], OnDuplicate::Ignore)
                    .await?;
                Ok(())
            })
        })
        .await
    }

    /// The job: look the account up, issue the token, request the mail.
    pub(crate) async fn fulfil_link(self: &Arc<Self>, request: LinkRequest) -> Result<(), Failure> {
        let core = self.clone();
        self.in_tx(move |tx| {
            Box::pin(async move {
                let LinkRequest {
                    purpose,
                    email,
                    return_to,
                    browser,
                    client,
                } = request;
                let user = core.user_by_email(tx, email.normalized(), true).await?;
                let (token_purpose, audit, ttl, base, topic_name) = match purpose {
                    LinkPurpose::Verification => match &user {
                        Some(u) if !u.verified() && u.active() => (
                            Purpose::EmailVerification,
                            None,
                            core.settings.tokens.email_verification_ttl,
                            &core.settings.links.verify_email,
                            topic::VERIFY_EMAIL,
                        ),
                        _ => return Ok(()),
                    },
                    LinkPurpose::PasswordReset => match &user {
                        Some(u) if u.active() && core.settings.password.is_some() => (
                            Purpose::PasswordReset,
                            Some(
                                Audit::new("user.password_reset_requested", Actor::Anonymous)
                                    .target(u.id),
                            ),
                            core.settings.tokens.password_reset_ttl,
                            &core.settings.links.password_reset,
                            topic::PASSWORD_RESET,
                        ),
                        _ => return Ok(()),
                    },
                    LinkPurpose::MagicLink => {
                        let audit = match &user {
                            Some(u) if u.active() => {
                                Audit::new("magic_link.requested", Actor::Anonymous).target(u.id)
                            }
                            Some(_) => return Ok(()),
                            None if core.settings.magic_links == Some(true)
                                && core.settings.registration == Registration::Open =>
                            {
                                Audit::new("magic_link.requested", Actor::Anonymous)
                                    .subject(core.keys.subject(email.normalized()))
                            }
                            None => return Ok(()),
                        };
                        (
                            Purpose::MagicLink,
                            Some(audit),
                            core.settings.tokens.magic_link_ttl,
                            &core.settings.links.magic_link,
                            topic::MAGIC_LINK,
                        )
                    }
                };
                let bound = Bound {
                    browser: if purpose == LinkPurpose::Verification {
                        browser
                    } else {
                        None
                    },
                    session: None,
                };
                let (token_id, token, expires) = core
                    .issue_token(
                        tx,
                        token_purpose,
                        email.normalized(),
                        user.as_ref().map(|u| u.id),
                        &email,
                        return_to.as_deref(),
                        bound,
                        ttl,
                    )
                    .await?;
                let to = user.as_ref().map_or(email.display(), |u| u.email.as_str());
                core.notify(
                    tx,
                    topic_name,
                    Core::address(to, user.as_ref())?,
                    Core::data(user.as_ref(), Some(expires)),
                    Some((Core::link(base, &token), purpose.as_str())),
                    token_id,
                    Some(expires),
                    &client,
                )
                .await?;
                if let Some(audit) = audit {
                    core.audit(tx, audit, &client).await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| e.failure)
    }
}

/// Read a queued request back; `None` for a payload this version cannot
/// read, which is a permanent failure.
fn parse(payload: &Value) -> Option<LinkRequest> {
    let purpose = LinkPurpose::parse(payload["purpose"].as_str()?)?;
    let email = normalize::email(payload["email"].as_str()?).ok()?;
    let return_to = match &payload["return_to"] {
        Value::Null => None,
        Value::String(path) => Some(normalize::return_to(path).ok()?),
        _ => return None,
    };
    let browser = match &payload["browser"] {
        Value::Null => None,
        Value::String(text) => {
            let bytes = URL_SAFE_NO_PAD.decode(text).ok()?;
            Some(<[u8; 32]>::try_from(bytes.as_slice()).ok()?)
        }
        _ => return None,
    };
    Some(LinkRequest {
        purpose,
        email,
        return_to,
        browser,
        client: Client {
            ip: payload["ip"].as_str().and_then(|ip| ip.parse().ok()),
            user_agent: payload["user_agent"]
                .as_str()
                .and_then(|ua| Client::user_agent(Some(ua))),
            request_id: payload["request_id"]
                .as_str()
                .filter(|r| merezha_domain::valid_name(r))
                .map(str::to_owned),
        },
    })
}

/// The `identity.link_request` handler; it waits for the block to start.
pub(crate) struct LinkRequestJob {
    pub(crate) core: Arc<OnceLock<Arc<Core>>>,
}
impl JobHandler for LinkRequestJob {
    fn handle<'a>(
        &'a self,
        _: JobContext,
        payload: Value,
    ) -> BoxFuture<'a, Result<(), JobFailure>> {
        Box::pin(async move {
            let Some(core) = self.core.get() else {
                return Err(JobFailure::release());
            };
            let request =
                parse(&payload).ok_or_else(|| JobFailure::permanent("invalid_job_payload"))?;
            core.fulfil_link(request).await.map_err(JobFailure::from)
        })
    }
}
