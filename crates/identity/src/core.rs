//! The running identity block: settings, keys, ports and the building
//! blocks every use case shares (transactions, rate limits, audit rows,
//! notification requests, sessions and the JSON shapes of section 4).
use crate::{
    config::Settings,
    gcra::{self, Charge, Policy, Rate},
    hasher::Hasher,
    ids,
    metrics::Metrics,
    normalize,
    password::PasswordPolicy,
    secrets::{Keys, RawToken, SESSION_PREFIX},
};
use jiff::{SignedDuration, Timestamp};
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    AppContext, BoxFuture, Database, Tx,
    errors::FieldError,
    ports::{
        Address, Channel, ChannelSelection, Correlation, DedupeKey, EmailAddress, Locale,
        MessageContext, NewEvent, NotificationRequest, Notifier, Recipient, SourceOutbox,
    },
};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Map, Value, json};
use std::{
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use uuid::Uuid;

/// Bound on every request's database work.
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(5);
/// Longest user agent kept, in bytes.
pub const MAX_USER_AGENT: usize = 512;

/// Background audit writes in flight at most (break-glass, organization
/// entry).
pub const AUDIT_SLOTS: usize = 64;
/// Break-glass audit writes running in the background at most (current-
/// thread runtimes only); they share nothing with [`AUDIT_SLOTS`].
pub const BREAK_GLASS_SLOTS: usize = 8;

/// The module name: section, route prefix, ledger key, outbox source.
pub const NAME: &str = "identity";

/// Notification topics identity owns (R2, section 8).
pub mod topic {
    pub const VERIFY_EMAIL: &str = "identity.verify_email";
    pub const PASSWORD_RESET: &str = "identity.password_reset";
    pub const MAGIC_LINK: &str = "identity.magic_link";
    pub const ACCOUNT_EXISTS: &str = "identity.account_exists";
    pub const PASSWORD_CHANGED: &str = "identity.password_changed";
    pub const EMAIL_CHANGE_CONFIRM: &str = "identity.email_change_confirm";
    pub const EMAIL_CHANGE_NOTICE: &str = "identity.email_change_notice";
    pub const INVITATION: &str = "identity.invitation";
    pub const NEW_SIGN_IN: &str = "identity.new_sign_in";
    /// Every topic, NT predeclares them (R2).
    pub const ALL: [&str; 9] = [
        VERIFY_EMAIL,
        PASSWORD_RESET,
        MAGIC_LINK,
        ACCOUNT_EXISTS,
        PASSWORD_CHANGED,
        EMAIL_CHANGE_CONFIRM,
        EMAIL_CHANGE_NOTICE,
        INVITATION,
        NEW_SIGN_IN,
    ];
}

/// Integration events identity publishes (R11, section 8), all at
/// version 1. Payloads carry ids, never personal data.
pub mod event {
    pub const USER_REGISTERED: &str = "identity.user_registered";
    pub const USER_EMAIL_VERIFIED: &str = "identity.user_email_verified";
    pub const USER_EMAIL_CHANGED: &str = "identity.user_email_changed";
    pub const USER_DISABLED: &str = "identity.user_disabled";
    pub const USER_ENABLED: &str = "identity.user_enabled";
    pub const USER_DELETED: &str = "identity.user_deleted";
    pub const ORGANIZATION_CREATED: &str = "identity.organization_created";
    pub const ORGANIZATION_UPDATED: &str = "identity.organization_updated";
    pub const ORGANIZATION_DELETED: &str = "identity.organization_deleted";
    pub const MEMBERSHIP_CREATED: &str = "identity.membership_created";
    pub const MEMBERSHIP_ROLES_CHANGED: &str = "identity.membership_roles_changed";
    pub const MEMBERSHIP_REMOVED: &str = "identity.membership_removed";
    pub const USER_APPLICATION_ROLES_CHANGED: &str = "identity.user_application_roles_changed";
    pub const ALL: [&str; 13] = [
        USER_REGISTERED,
        USER_EMAIL_VERIFIED,
        USER_EMAIL_CHANGED,
        USER_DISABLED,
        USER_ENABLED,
        USER_DELETED,
        ORGANIZATION_CREATED,
        ORGANIZATION_UPDATED,
        ORGANIZATION_DELETED,
        MEMBERSHIP_CREATED,
        MEMBERSHIP_ROLES_CHANGED,
        MEMBERSHIP_REMOVED,
        USER_APPLICATION_ROLES_CHANGED,
    ];
}

/// A failure with optional field details and `Retry-After`, the HTTP
/// error of a use case.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiError {
    pub failure: Failure,
    pub details: Vec<FieldError>,
    pub retry_after: Option<Duration>,
}
impl ApiError {
    pub fn code(code: &str) -> Self {
        Failure::permanent(code).into()
    }
    pub fn conflict(code: &str) -> Self {
        Failure::conflict(code).into()
    }
    /// `validation_failed` with one detail per field.
    pub fn invalid(details: Vec<(&str, &str)>) -> Self {
        Self {
            failure: Failure::permanent("validation_failed"),
            details: details
                .into_iter()
                .filter_map(|(path, code)| FieldError::new(path, code).ok())
                .collect(),
            retry_after: None,
        }
    }
    pub fn rate_limited(retry_after: Duration) -> Self {
        Self {
            failure: Failure::transient("rate_limited"),
            details: Vec::new(),
            retry_after: Some(retry_after),
        }
    }
}
impl From<Failure> for ApiError {
    fn from(failure: Failure) -> Self {
        Self {
            failure,
            details: Vec::new(),
            retry_after: None,
        }
    }
}

/// What the edge learned about the caller. The address and user agent go
/// to session and audit rows only, never to logs or telemetry.
#[derive(Clone, Debug, Default)]
pub struct Client {
    pub ip: Option<IpAddr>,
    pub user_agent: Option<String>,
    pub request_id: Option<String>,
}
impl Client {
    pub fn user_agent(raw: Option<&str>) -> Option<String> {
        let raw = raw?;
        let mut end = raw.len().min(MAX_USER_AGENT);
        while !raw.is_char_boundary(end) {
            end -= 1;
        }
        let kept: String = raw[..end].chars().filter(|c| !c.is_control()).collect();
        (!kept.is_empty()).then_some(kept)
    }
    /// The correlation id of requests this client causes.
    pub(crate) fn correlation(&self, random: &dyn merezha_platform::Random) -> Identity {
        if let Some(id) = self
            .request_id
            .as_deref()
            .and_then(|r| Identity::new(r).ok())
        {
            return id;
        }
        let token =
            merezha_platform::context::random_token(random).unwrap_or_else(|_| "identity".into());
        Identity::new(token).unwrap_or_else(|_| unreachable!("hex is a valid identity"))
    }
}

/// How a session's token travels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Cookie,
    Bearer,
}
impl Transport {
    pub fn as_str(self) -> &'static str {
        match self {
            Transport::Cookie => "cookie",
            Transport::Bearer => "bearer",
        }
    }
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "cookie" => Some(Transport::Cookie),
            "bearer" => Some(Transport::Bearer),
            _ => None,
        }
    }
}

/// How a session was established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Password,
    MagicLink,
    EmailToken,
    /// An invitation accepted anonymously (its token proves the inbox).
    Invitation,
}
impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Password => "password",
            Method::MagicLink => "magic_link",
            Method::EmailToken => "email_token",
            Method::Invitation => "invitation",
        }
    }
}

/// One-time token purposes (section 6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Purpose {
    EmailVerification,
    MagicLink,
    PasswordReset,
    EmailChange,
}
impl Purpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Purpose::EmailVerification => "email_verification",
            Purpose::MagicLink => "magic_link",
            Purpose::PasswordReset => "password_reset",
            Purpose::EmailChange => "email_change",
        }
    }
}

/// A `users` row with its password flag.
#[derive(Clone)]
pub struct UserRow {
    pub id: Uuid,
    pub email: String,
    pub email_normalized: String,
    pub verified_at: Option<Timestamp>,
    pub display_name: Option<String>,
    pub locale: Option<String>,
    pub status: String,
    pub created_at: Timestamp,
    pub phc: Option<String>,
}
impl std::fmt::Debug for UserRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserRow")
            .field("id", &self.id)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}
impl UserRow {
    pub fn active(&self) -> bool {
        self.status == "active"
    }
    pub fn verified(&self) -> bool {
        self.verified_at.is_some()
    }
    pub fn identity(&self) -> Identity {
        ids::identity(ids::USER, self.id)
    }
    /// The `user` shape of section 4.
    pub fn json(&self) -> Value {
        json!({
            "id": ids::render(ids::USER, self.id),
            "email": self.email,
            "email_verified": self.verified(),
            "display_name": self.display_name,
            "locale": self.locale,
            "created_at": self.created_at.to_string(),
            "credentials": {"password": self.phc.is_some(), "passkeys": 0, "oauth": []},
        })
    }
}
pub(crate) fn user_row(row: &tokio_postgres::Row) -> UserRow {
    UserRow {
        id: row.get(0),
        email: row.get(1),
        email_normalized: row.get(2),
        verified_at: row.get(3),
        display_name: row.get(4),
        locale: row.get(5),
        status: row.get(6),
        created_at: row.get(7),
        phc: row.get(8),
    }
}

/// A `sessions` row.
#[derive(Clone)]
pub struct SessionRow {
    pub id: Uuid,
    pub user_id: Uuid,
    pub transport: Transport,
    pub method: String,
    pub created_at: Timestamp,
    pub authenticated_at: Timestamp,
    pub last_seen_at: Timestamp,
    pub idle_expires_at: Timestamp,
    pub absolute_expires_at: Timestamp,
    pub ip: Option<IpAddr>,
    pub user_agent: Option<String>,
    /// The organization selected with `PUT /session/organization`.
    pub active_organization: Option<Uuid>,
}
impl std::fmt::Debug for SessionRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionRow(<redacted>)")
    }
}
pub(crate) fn session_row(row: &tokio_postgres::Row, at: usize) -> SessionRow {
    SessionRow {
        id: row.get(at),
        user_id: row.get(at + 1),
        transport: Transport::parse(row.get(at + 2)).unwrap_or(Transport::Cookie),
        method: row.get(at + 3),
        created_at: row.get(at + 4),
        authenticated_at: row.get(at + 5),
        last_seen_at: row.get(at + 6),
        idle_expires_at: row.get(at + 7),
        absolute_expires_at: row.get(at + 8),
        ip: row.get(at + 9),
        user_agent: row.get(at + 10),
        active_organization: row.get(at + 11),
    }
}
impl SessionRow {
    pub fn alive(&self, now: Timestamp) -> bool {
        now < self.idle_expires_at && now < self.absolute_expires_at
    }
    /// The `session` shape of section 4.
    pub fn json(&self, current: bool) -> Value {
        json!({
            "id": ids::render(ids::SESSION, self.id),
            "method": self.method,
            "transport": self.transport.as_str(),
            "created_at": self.created_at.to_string(),
            "authenticated_at": self.authenticated_at.to_string(),
            "idle_expires_at": self.idle_expires_at.to_string(),
            "absolute_expires_at": self.absolute_expires_at.to_string(),
            "active_organization_id": self
                .active_organization
                .map(|org| Value::String(ids::render(ids::ORGANIZATION, org)))
                .unwrap_or(Value::Null),
            "current": current,
        })
    }
    /// The listing shape: the address truncated to /24 or /48 and the user
    /// agent, so users recognize devices without full addresses.
    pub fn listing(&self, current: bool) -> Value {
        let mut value = self.json(current);
        value["ip"] = self.ip.map(truncate_ip).map_or(Value::Null, Value::String);
        value["user_agent"] = self.user_agent.clone().map_or(Value::Null, Value::String);
        value
    }
}

/// `a.b.c.0/24` or the first three IPv6 groups `/48`.
pub fn truncate_ip(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            format!("{a}.{b}.{c}.0/24")
        }
        IpAddr::V6(v6) => {
            let s = v6.segments();
            format!("{:x}:{:x}:{:x}::/48", s[0], s[1], s[2])
        }
    }
}

/// A session just created: its row and its token, which appears once in
/// the response or the cookie.
pub struct NewSession {
    pub row: SessionRow,
    pub token: SecretString,
}
impl std::fmt::Debug for NewSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NewSession(<redacted>)")
    }
}

/// Who an audit row is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Actor {
    User(Uuid),
    /// An API key (its id), whatever its owner.
    ApiKey(Uuid),
    Anonymous,
    System,
}

/// One audit row (section 7). `details` keys are allowlisted.
#[derive(Clone, Debug)]
pub struct Audit {
    pub action: &'static str,
    pub outcome: &'static str,
    pub actor: Actor,
    /// What the row is about: `(kind, id)` such as `("user", id)`.
    pub target: Option<(&'static str, Uuid)>,
    /// The organization the row belongs to, listed by its audit route.
    pub organization: Option<Uuid>,
    /// HMAC of the address for rows about unknown accounts.
    pub subject: Option<[u8; 32]>,
    pub details: Value,
}
/// The only keys audit `details` may carry: methods, transports, reason
/// codes, policy names, counts, scopes, role keys, permission names,
/// resource kinds and statuses. Free text, tokens and emails never.
pub const AUDIT_DETAIL_KEYS: [&str; 13] = [
    "method",
    "transport",
    "reason",
    "policy",
    "count",
    "keep_current",
    "scope",
    "role_keys",
    "permissions",
    "permission",
    "resource_kind",
    "status",
    "keep_owner",
];
impl Audit {
    pub fn new(action: &'static str, actor: Actor) -> Self {
        Self {
            action,
            outcome: "succeeded",
            actor,
            target: match actor {
                Actor::User(id) => Some(("user", id)),
                _ => None,
            },
            organization: None,
            subject: None,
            details: json!({}),
        }
    }
    pub fn failed(mut self) -> Self {
        self.outcome = "failed";
        self
    }
    pub fn denied(mut self) -> Self {
        self.outcome = "denied";
        self
    }
    pub fn target(mut self, user: Uuid) -> Self {
        self.target = Some(("user", user));
        self
    }
    /// A target other than a user, such as `("api_key", id)`.
    pub fn target_of(mut self, kind: &'static str, id: Uuid) -> Self {
        self.target = Some((kind, id));
        self
    }
    pub fn organization(mut self, organization: Uuid) -> Self {
        self.organization = Some(organization);
        self
    }
    pub fn subject(mut self, subject: [u8; 32]) -> Self {
        self.subject = Some(subject);
        self
    }
    pub fn detail(mut self, key: &'static str, value: impl Into<Value>) -> Self {
        debug_assert!(AUDIT_DETAIL_KEYS.contains(&key), "{key}");
        if AUDIT_DETAIL_KEYS.contains(&key) {
            self.details[key] = value.into();
        }
        self
    }
}

/// A rate-limit subject; emails are normalized, IPv6 addresses keyed by
/// their /64. None of it reaches the table: only the HMAC does.
#[derive(Clone, Copy, Debug)]
pub enum Subject<'a> {
    Ip(Option<IpAddr>),
    Email(&'a str),
    EmailIp(&'a str, Option<IpAddr>),
    Session(Uuid),
    /// An organization, an API key or another row.
    Id(Uuid),
}
fn ip_key(ip: Option<IpAddr>) -> Vec<u8> {
    match ip.map(|ip| ip.to_canonical()) {
        Some(IpAddr::V4(v4)) => v4.octets().to_vec(),
        Some(IpAddr::V6(v6)) => v6.octets()[..8].to_vec(),
        None => b"unknown".to_vec(),
    }
}
impl Subject<'_> {
    fn bytes(&self) -> Vec<u8> {
        match self {
            Subject::Ip(ip) => ip_key(*ip),
            Subject::Email(email) => email.as_bytes().to_vec(),
            Subject::EmailIp(email, ip) => {
                let mut bytes = email.as_bytes().to_vec();
                bytes.push(0);
                bytes.extend(ip_key(*ip));
                bytes
            }
            Subject::Session(id) | Subject::Id(id) => id.as_bytes().to_vec(),
        }
    }
}

/// Everything a use case needs. Built once at module start.
pub struct Core {
    pub settings: Settings,
    pub keys: Keys,
    pub ctx: AppContext,
    pub notifier: Arc<dyn Notifier>,
    pub outbox: SourceOutbox,
    pub hasher: Option<Hasher>,
    pub policy: Option<PasswordPolicy>,
    pub metrics: Metrics,
    /// The role definitions of this configuration (section 5).
    pub rbac: crate::rbac::Policy,
    pub grants: crate::rbac::GrantCache,
    pub audited: crate::rbac::Once,
    pub audit_slots: Arc<tokio::sync::Semaphore>,
    /// Break-glass uses whose audit row is committed (section 5).
    pub break_glass: crate::rbac::Once,
    /// Break-glass audit writes in flight in the background.
    pub break_glass_pending: crate::rbac::Once,
    pub break_glass_slots: Arc<tokio::sync::Semaphore>,
    /// Resource kinds assignments may target (`unknown_resource_kind`).
    pub resource_kinds: std::collections::BTreeSet<String>,
}
impl std::fmt::Debug for Core {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Core")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// `ts + d`, saturating at the largest timestamp.
pub fn plus(ts: Timestamp, d: Duration) -> Timestamp {
    SignedDuration::try_from(d)
        .ok()
        .and_then(|d| ts.checked_add(d).ok())
        .unwrap_or(Timestamp::MAX)
}
/// `ts - d`, saturating at the smallest timestamp.
pub fn minus(ts: Timestamp, d: Duration) -> Timestamp {
    SignedDuration::try_from(d)
        .ok()
        .and_then(|d| ts.checked_sub(d).ok())
        .unwrap_or(Timestamp::MIN)
}
pub(crate) fn sql(error: tokio_postgres::Error) -> Failure {
    merezha_postgres::classify(&error)
}

impl Core {
    pub fn db(&self) -> &Database {
        self.ctx.database()
    }
    pub fn now(&self) -> Timestamp {
        Timestamp::try_from(self.ctx.clock().now()).unwrap_or(Timestamp::UNIX_EPOCH)
    }
    pub fn deadline(&self) -> Instant {
        Instant::now() + REQUEST_DEADLINE
    }
    pub fn new_id(&self) -> Result<Uuid, Failure> {
        ids::new_v7(self.ctx.clock().now(), self.ctx.random())
    }

    /// Run `work` in one transaction; the error's details and
    /// `Retry-After` survive the rollback.
    pub async fn in_tx<T, F>(self: &Arc<Self>, work: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: for<'t> FnOnce(&'t mut Tx<'_>) -> BoxFuture<'t, Result<T, ApiError>> + Send,
    {
        let slot: Arc<Mutex<Option<ApiError>>> = Arc::new(Mutex::new(None));
        let kept = slot.clone();
        let result = self
            .db()
            .transaction(self.deadline(), move |tx| {
                let work = work(tx);
                Box::pin(async move {
                    work.await.map_err(|error| {
                        let failure = error.failure.clone();
                        *kept
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
                        failure
                    })
                })
            })
            .await;
        match result {
            Ok(value) => Ok(value),
            Err(failure) => match slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
            {
                Some(error) if error.failure == failure => Err(error),
                _ => Err(failure.into()),
            },
        }
    }

    // ----- rate limits (section 7) -----

    /// Charge `policy` for `subject`: denied fails `rate_limited` with
    /// `Retry-After`, audited once per bucket per period.
    pub async fn charge(
        self: &Arc<Self>,
        policy: Policy,
        subject: Subject<'_>,
        client: &Client,
    ) -> Result<(), ApiError> {
        let rate = self.settings.rate(&policy);
        let bucket = self.keys.bucket(policy.name, &subject.bytes());
        match self.gcra_charge(&bucket, rate).await? {
            None => Ok(()),
            Some(retry) => {
                self.limited(policy, rate, &bucket, client).await;
                Err(ApiError::rate_limited(retry))
            }
        }
    }
    /// Check `policy` without charging it.
    pub async fn peek(
        self: &Arc<Self>,
        policy: Policy,
        subject: Subject<'_>,
        client: &Client,
    ) -> Result<(), ApiError> {
        debug_assert_eq!(policy.charge, Charge::Failure);
        let rate = self.settings.rate(&policy);
        let bucket = self.keys.bucket(policy.name, &subject.bytes());
        let now = self.now().as_microsecond();
        let tat = self.read_tat(&bucket).await?;
        match gcra::admit(tat, now, rate) {
            Ok(_) => Ok(()),
            Err(retry) => {
                self.limited(policy, rate, &bucket, client).await;
                Err(ApiError::rate_limited(micros(retry)))
            }
        }
    }
    /// [`Core::peek`] for several policies in one statement.
    pub async fn peek_many(
        self: &Arc<Self>,
        checks: &[(Policy, Subject<'_>)],
        client: &Client,
    ) -> Result<(), ApiError> {
        let buckets: Vec<Vec<u8>> = checks
            .iter()
            .map(|(policy, subject)| self.keys.bucket(policy.name, &subject.bytes()).to_vec())
            .collect();
        let rows = {
            let session = self.db().acquire(self.deadline()).await?;
            self.db()
                .query(&session, &crate::sql::RATE_READ_MANY, &[&buckets])
                .await
                .map_err(sql)?
        };
        let now = self.now().as_microsecond();
        for ((policy, _), bucket) in checks.iter().zip(&buckets) {
            let tat = rows
                .iter()
                .find(|r| r.get::<_, &[u8]>(0) == bucket.as_slice())
                .map(|r| r.get::<_, Timestamp>(1).as_microsecond());
            let rate = self.settings.rate(policy);
            if let Err(retry) = gcra::admit(tat, now, rate) {
                let mut key = [0u8; 32];
                key.copy_from_slice(bucket);
                self.limited(*policy, rate, &key, client).await;
                return Err(ApiError::rate_limited(micros(retry)));
            }
        }
        Ok(())
    }
    /// Charge a failure-charged policy after the work failed. A full bucket
    /// is no error here: the request already failed.
    pub async fn charge_failure(&self, policy: Policy, subject: Subject<'_>) {
        let rate = self.settings.rate(&policy);
        let bucket = self.keys.bucket(policy.name, &subject.bytes());
        let _ = self.gcra_charge(&bucket, rate).await;
    }
    async fn read_tat(&self, bucket: &[u8; 32]) -> Result<Option<i64>, Failure> {
        let client = self.db().acquire(self.deadline()).await?;
        let b: &[u8] = bucket;
        let row = self
            .db()
            .query_opt(&client, &crate::sql::RATE_READ, &[&b])
            .await
            .map_err(sql)?;
        Ok(row.map(|r| r.get::<_, Timestamp>(0).as_microsecond()))
    }
    /// One GCRA statement; `Some(retry_after)` when denied.
    async fn gcra_charge(
        &self,
        bucket: &[u8; 32],
        rate: Rate,
    ) -> Result<Option<Duration>, Failure> {
        let now = self.now();
        let client = self.db().acquire(self.deadline()).await?;
        let b: &[u8] = bucket;
        let interval = rate.interval_us();
        let burst = rate.burst_us();
        let row = self
            .db()
            .query_opt(
                &client,
                &crate::sql::RATE_CHARGE,
                &[&b, &now, &interval, &burst],
            )
            .await
            .map_err(sql)?;
        if row.is_some() {
            return Ok(None);
        }
        let tat = self
            .db()
            .query_opt(&client, &crate::sql::RATE_READ, &[&b])
            .await
            .map_err(sql)?
            .map(|r| r.get::<_, Timestamp>(0).as_microsecond());
        let retry = gcra::admit(tat, now.as_microsecond(), rate)
            .err()
            .unwrap_or(1_000_000);
        Ok(Some(micros(retry)))
    }
    /// The metric for every denial, and one `rate_limit.exceeded` audit row
    /// per bucket per period, so a flood cannot fill the table.
    async fn limited(
        self: &Arc<Self>,
        policy: Policy,
        rate: Rate,
        bucket: &[u8; 32],
        client: &Client,
    ) {
        self.metrics.rate_limited(policy.name);
        let marker = self.keys.bucket("audit.rate_limit", bucket);
        let once = Rate::new(1, rate.period);
        if let Ok(None) = self.gcra_charge(&marker, once).await {
            let audit = Audit::new("rate_limit.exceeded", Actor::Anonymous)
                .denied()
                .detail("policy", policy.name);
            self.audit_alone(audit, client).await;
        }
    }

    // ----- audit (section 7) -----

    /// Append `audit` inside the caller's transaction; a failure fails the
    /// state change.
    pub async fn audit(
        &self,
        tx: &mut Tx<'_>,
        audit: Audit,
        client: &Client,
    ) -> Result<(), Failure> {
        let id = self.new_id()?;
        let now = self.now();
        let (actor_kind, actor_id) = match audit.actor {
            Actor::User(id) => ("user", Some(id)),
            Actor::ApiKey(id) => ("api_key", Some(id)),
            Actor::Anonymous => ("anonymous", None),
            Actor::System => ("system", None),
        };
        let subject: Option<&[u8]> = audit.subject.as_ref().map(|s| s.as_slice());
        let target_kind = audit.target.map(|(kind, _)| kind);
        let target_id = audit.target.map(|(_, id)| id);
        self.db()
            .execute(
                &*tx,
                &crate::sql::AUDIT_INSERT,
                &[
                    &id,
                    &now,
                    &audit.action,
                    &audit.outcome,
                    &actor_kind,
                    &actor_id,
                    &target_kind,
                    &target_id,
                    &subject,
                    &client.ip,
                    &client.user_agent,
                    &client.request_id,
                    &audit.details,
                    &audit.organization,
                ],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }
    /// Append `audit` in its own short transaction, for failures and
    /// denials. A failed write is counted and logged, never raised.
    pub async fn audit_alone(self: &Arc<Self>, audit: Audit, client: &Client) {
        let core = self.clone();
        let client_owned = client.clone();
        let result = self
            .db()
            .transaction(self.deadline(), move |tx| {
                Box::pin(async move { core.audit(tx, audit, &client_owned).await })
            })
            .await;
        if let Err(failure) = result {
            self.metrics.audit_failure();
            merezha_telemetry::event!(
                ERROR,
                "identity.audit_write_failed",
                code = failure.code.as_str()
            );
        }
    }

    // ----- integration events (R11) -----

    pub async fn publish(
        &self,
        tx: &mut Tx<'_>,
        topic: &'static str,
        dedupe: String,
        payload: Value,
        client: &Client,
    ) -> Result<(), Failure> {
        self.publish_in(tx, topic, dedupe, payload, client, None)
            .await
    }
    /// [`Core::publish`] with the envelope's `tenant`, the organization the
    /// event belongs to.
    pub async fn publish_in(
        &self,
        tx: &mut Tx<'_>,
        topic: &'static str,
        dedupe: String,
        payload: Value,
        client: &Client,
        tenant: Option<Uuid>,
    ) -> Result<(), Failure> {
        let record = self.outbox.event(
            Identity::new(topic)?,
            NewEvent {
                dedupe_key: DedupeKey::new(dedupe)?,
                version: 1,
                occurred_at: self.now(),
                payload,
                context: MessageContext {
                    correlation_id: client.correlation(self.ctx.random()),
                    causation_id: None,
                    tenant: tenant.map(|t| ids::identity(ids::ORGANIZATION, t)),
                },
            },
        );
        self.outbox
            .append(tx, &[record], merezha_platform::ports::OnDuplicate::Ignore)
            .await?;
        Ok(())
    }

    // ----- notifications (R2, section 8) -----

    /// The link of `base` carrying `token` in its fragment.
    pub fn link(base: &str, token: &RawToken) -> SecretString {
        SecretString::from(format!("{base}#token={}", token.encode().expose_secret()))
    }

    /// Request `topic` for `recipient` inside the caller's transaction.
    /// Raw links travel only in `secret_data`; with `dev_print_links` on
    /// loopback the link also goes to stderr as `identity.dev_link`.
    #[allow(clippy::too_many_arguments)]
    pub async fn notify(
        &self,
        tx: &mut Tx<'_>,
        topic: &'static str,
        recipient: Recipient,
        data: Map<String, Value>,
        link: Option<(SecretString, &'static str)>,
        dedupe: Uuid,
        expires_at: Option<Timestamp>,
        client: &Client,
    ) -> Result<(), Failure> {
        let mut secret_data = Map::new();
        if let Some((link, _)) = &link {
            secret_data.insert(
                "link".into(),
                Value::String(link.expose_secret().to_owned()),
            );
        }
        let request = NotificationRequest {
            topic: Identity::new(topic)?,
            recipients: vec![recipient],
            channels: ChannelSelection::Only(vec![Channel::Email]),
            template: None,
            data,
            secret_data,
            locale: None,
            dedupe_key: DedupeKey::new(format!("{topic}:{}", dedupe.simple()))?,
            tenant: None,
            members: Vec::new(),
            not_before: None,
            expires_at,
            collapse_key: None,
            correlation: Correlation {
                correlation_id: client.correlation(self.ctx.random()),
                causation_id: None,
            },
        };
        self.notifier
            .request(tx, request)
            .await
            .map_err(Failure::from)?;
        if let Some((link, purpose)) = link
            && self.settings.dev_print_links
        {
            merezha_telemetry::event!(
                INFO,
                "identity.dev_link",
                purpose,
                link = link.expose_secret()
            );
        }
        Ok(())
    }
    /// The explicit address recipient of token mail.
    pub fn address(email: &str, user: Option<&UserRow>) -> Result<Recipient, Failure> {
        let parsed =
            normalize::email(email).map_err(|_| Failure::permanent("invalid_recipient"))?;
        Ok(Recipient::Address(Address::Email {
            address: EmailAddress::new(parsed.deliverable())?,
            name: user.and_then(|u| u.display_name.clone()),
            user: user.map(UserRow::identity),
            locale: user
                .and_then(|u| u.locale.as_deref())
                .and_then(|l| Locale::new(l).ok()),
        }))
    }
    /// Non-secret template data.
    pub fn data(user: Option<&UserRow>, expires_at: Option<Timestamp>) -> Map<String, Value> {
        let mut data = Map::new();
        if let Some(name) = user.and_then(|u| u.display_name.clone()) {
            data.insert("display_name".into(), Value::String(name));
        }
        if let Some(expires_at) = expires_at {
            data.insert("expires_at".into(), Value::String(expires_at.to_string()));
        }
        data
    }

    // ----- users -----

    /// The user with this normalized email, locked when `lock`.
    pub async fn user_by_email(
        &self,
        tx: &mut Tx<'_>,
        normalized: &str,
        lock: bool,
    ) -> Result<Option<UserRow>, Failure> {
        let row = if lock {
            self.db()
                .query_opt(&*tx, &crate::sql::USER_BY_EMAIL_LOCKED, &[&normalized])
                .await
        } else {
            self.db()
                .query_opt(&*tx, &crate::sql::USER_BY_EMAIL, &[&normalized])
                .await
        };
        Ok(row.map_err(sql)?.map(|r| user_row(&r)))
    }
    pub async fn user_by_id(
        &self,
        tx: &mut Tx<'_>,
        id: Uuid,
        lock: bool,
    ) -> Result<Option<UserRow>, Failure> {
        let row = if lock {
            self.db()
                .query_opt(&*tx, &crate::sql::USER_BY_ID_LOCKED, &[&id])
                .await
        } else {
            self.db()
                .query_opt(&*tx, &crate::sql::USER_BY_ID, &[&id])
                .await
        };
        Ok(row.map_err(sql)?.map(|r| user_row(&r)))
    }
    /// Read a user outside any transaction.
    pub async fn read_user_by_email(&self, normalized: &str) -> Result<Option<UserRow>, Failure> {
        let client = self.db().acquire(self.deadline()).await?;
        Ok(self
            .db()
            .query_opt(&client, &crate::sql::USER_BY_EMAIL, &[&normalized])
            .await
            .map_err(sql)?
            .map(|r| user_row(&r)))
    }
    pub async fn read_user(&self, id: Uuid) -> Result<Option<UserRow>, Failure> {
        let client = self.db().acquire(self.deadline()).await?;
        Ok(self
            .db()
            .query_opt(&client, &crate::sql::USER_BY_ID, &[&id])
            .await
            .map_err(sql)?
            .map(|r| user_row(&r)))
    }

    /// The pre-hijacking rule (section 2): the real owner of an unverified
    /// account's address proved control of it. Strip every credential,
    /// session and token of the account and audit
    /// `user.unverified_account_claimed`; the caller then marks the address
    /// verified and continues the flow. The caller holds the user lock.
    pub async fn claim_unverified(
        &self,
        tx: &mut Tx<'_>,
        user: &mut UserRow,
        client: &Client,
    ) -> Result<(), Failure> {
        let now = self.now();
        self.db()
            .execute(&*tx, &crate::sql::PASSWORD_DELETE, &[&user.id])
            .await
            .map_err(sql)?;
        let revoked = self
            .db()
            .execute(
                &*tx,
                &crate::sql::SESSIONS_OF_USER_DELETE,
                &[&user.id, &None::<Uuid>],
            )
            .await
            .map_err(sql)?;
        self.db()
            .execute(&*tx, &crate::sql::TOKENS_OF_USER_DELETE, &[&user.id])
            .await
            .map_err(sql)?;
        // Its API keys go too; PR 3 adds passkeys and OAuth identities.
        self.db()
            .execute(&*tx, &crate::sql::KEYS_DELETE_OF_USER, &[&user.id])
            .await
            .map_err(sql)?;
        self.db()
            .execute(&*tx, &crate::sql::USER_BUMP, &[&user.id, &now])
            .await
            .map_err(sql)?;
        user.phc = None;
        self.metrics.sessions_revoked("admin", revoked);
        self.audit(
            tx,
            Audit::new("user.unverified_account_claimed", Actor::User(user.id)),
            client,
        )
        .await
    }

    // ----- one-time tokens (section 6) -----

    /// Issue a token for `purpose` replacing the live one of the same
    /// subject key, so older links stop working.
    #[allow(clippy::too_many_arguments)]
    pub async fn issue_token(
        &self,
        tx: &mut Tx<'_>,
        purpose: Purpose,
        subject_key: &str,
        user: Option<Uuid>,
        email: &normalize::Email,
        return_to: Option<&str>,
        bound: Bound,
        ttl: Duration,
    ) -> Result<(Uuid, RawToken, Timestamp), Failure> {
        let id = self.new_id()?;
        let token = RawToken::generate(self.ctx.random())?;
        let digest = token.digest();
        let d: &[u8] = &digest;
        let now = self.now();
        let expires_at = plus(now, ttl);
        self.db()
            .execute(
                &*tx,
                &crate::sql::TOKEN_UPSERT,
                &[
                    &id,
                    &purpose.as_str(),
                    &d,
                    &subject_key,
                    &user,
                    &email.display(),
                    &email.normalized(),
                    &return_to,
                    &bound.session,
                    &now,
                    &expires_at,
                ],
            )
            .await
            .map_err(sql)?;
        self.metrics.token_issued(purpose.as_str());
        Ok((id, token, expires_at))
    }

    /// Consume a token of `purpose` with `DELETE ... RETURNING`: of two
    /// concurrent consumers one wins. Expired rows are deleted and then
    /// refused, so a token never gets a second try.
    pub async fn consume_token(
        &self,
        tx: &mut Tx<'_>,
        purpose: Purpose,
        token: &RawToken,
    ) -> Result<Option<ConsumedToken>, Failure> {
        let digest = token.digest();
        let d: &[u8] = &digest;
        // Lock order: the user row, then its tokens, as in every flow that
        // deletes a user's tokens under the user lock; consuming first
        // would deadlock against a concurrent password change.
        self.db()
            .execute(
                &*tx,
                &crate::sql::TOKEN_LOCK_OWNER,
                &[&d, &purpose.as_str()],
            )
            .await
            .map_err(sql)?;
        let row = self
            .db()
            .query_opt(&*tx, &crate::sql::TOKEN_CONSUME, &[&d, &purpose.as_str()])
            .await
            .map_err(sql)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let consumed = ConsumedToken {
            id: row.get(0),
            subject_key: row.get(1),
            user_id: row.get(2),
            email: row.get(3),
            email_normalized: row.get(4),
            return_to: row.get(5),
            expires_at: row.get(6),
            session: row.get(7),
        };
        if consumed.expires_at <= self.now() {
            return Ok(None);
        }
        Ok(Some(consumed))
    }

    /// A live or expired token of `purpose`, without consuming it.
    pub async fn peek_token(
        &self,
        tx: &mut Tx<'_>,
        purpose: Purpose,
        token: &RawToken,
    ) -> Result<Option<PendingToken>, Failure> {
        let digest = token.digest();
        let d: &[u8] = &digest;
        let row = self
            .db()
            .query_opt(&*tx, &crate::sql::TOKEN_PEEK, &[&d, &purpose.as_str()])
            .await
            .map_err(sql)?;
        Ok(row.map(|r| PendingToken {
            email: r.get(0),
            expires_at: r.get(1),
            session: r.get(2),
        }))
    }

    // ----- sessions (section 4) -----

    /// Create a session for `user` (whose row the caller locked), stamp
    /// `last_signed_in_at`, keep at most `max_per_user` sessions and audit
    /// `session.created`. `carry` keeps a rotated session's creation and
    /// absolute expiry.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_session(
        &self,
        tx: &mut Tx<'_>,
        user: Uuid,
        method: Method,
        transport: Transport,
        client: &Client,
        carry: Option<(Timestamp, Timestamp)>,
        audit: bool,
    ) -> Result<NewSession, Failure> {
        let id = self.new_id()?;
        let token = RawToken::generate(self.ctx.random())?;
        let digest = token.digest();
        let d: &[u8] = &digest;
        let now = self.now();
        let s = &self.settings.sessions;
        let (created_at, absolute) = carry.unwrap_or((now, plus(now, s.absolute_timeout)));
        let idle = plus(now, s.idle_timeout).min(absolute);
        let row = SessionRow {
            id,
            user_id: user,
            transport,
            method: method.as_str().to_owned(),
            created_at,
            authenticated_at: now,
            last_seen_at: now,
            idle_expires_at: idle,
            absolute_expires_at: absolute,
            ip: client.ip,
            user_agent: client.user_agent.clone(),
            active_organization: None,
        };
        self.db()
            .execute(
                &*tx,
                &crate::sql::SESSION_INSERT,
                &[
                    &id,
                    &d,
                    &user,
                    &transport.as_str(),
                    &method.as_str(),
                    &created_at,
                    &now,
                    &idle,
                    &absolute,
                    &client.ip,
                    &client.user_agent,
                ],
            )
            .await
            .map_err(sql)?;
        self.db()
            .execute(&*tx, &crate::sql::USER_SIGNED_IN, &[&user, &now])
            .await
            .map_err(sql)?;
        let cap = i64::from(s.max_per_user);
        let evicted = self
            .db()
            .execute(&*tx, &crate::sql::SESSION_EVICT, &[&user, &cap])
            .await
            .map_err(sql)?;
        self.metrics.sessions_revoked("expired", evicted);
        if audit {
            self.audit(
                tx,
                Audit::new("session.created", Actor::User(user))
                    .detail("method", method.as_str())
                    .detail("transport", transport.as_str()),
                client,
            )
            .await?;
        }
        self.metrics.session_created(method.as_str());
        Ok(NewSession {
            row,
            token: token.encode_with(SESSION_PREFIX),
        })
    }
    /// Replace session `old` with a new id and token (insert new, delete
    /// old, one transaction), keeping its creation and absolute expiry.
    /// `None` when `old` no longer exists.
    pub async fn rotate_session(
        &self,
        tx: &mut Tx<'_>,
        old: Uuid,
        method: Method,
        client: &Client,
    ) -> Result<Option<NewSession>, Failure> {
        let row = self
            .db()
            .query_opt(&*tx, &crate::sql::SESSION_TAKE, &[&old])
            .await
            .map_err(sql)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let previous = session_row(&row, 0);
        if !previous.alive(self.now()) {
            return Ok(None);
        }
        let created = self
            .create_session(
                tx,
                previous.user_id,
                method,
                previous.transport,
                client,
                Some((previous.created_at, previous.absolute_expires_at)),
                false,
            )
            .await?;
        // A pending email change follows its session to the new id; every
        // other session stays refused.
        self.db()
            .execute(&*tx, &crate::sql::TOKEN_REBIND, &[&old, &created.row.id])
            .await
            .map_err(sql)?;
        let mut created = created;
        if let Some(org) = previous.active_organization {
            self.db()
                .execute(
                    &*tx,
                    &crate::sql::SESSION_SET_ORGANIZATION,
                    &[&created.row.id, &Some(org)],
                )
                .await
                .map_err(sql)?;
            created.row.active_organization = Some(org);
        }
        Ok(Some(created))
    }
    /// The session row by id.
    pub async fn session(&self, tx: &mut Tx<'_>, id: Uuid) -> Result<Option<SessionRow>, Failure> {
        Ok(self
            .db()
            .query_opt(&*tx, &crate::sql::SESSION_BY_ID, &[&id])
            .await
            .map_err(sql)?
            .map(|r| session_row(&r, 0)))
    }
    /// The CSRF token of a session (section 4).
    pub fn csrf(&self, session: Uuid) -> String {
        self.keys.csrf_token(session)
    }
}

/// What a one-time token is bound to besides its subject: the session
/// that asked (email change tokens). The registering browser is bound on
/// the user row instead (`users.binding_hash`), so a verification resend
/// from any browser can never become "the same browser".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bound {
    pub session: Option<Uuid>,
}

/// A token looked at without consuming it.
#[derive(Clone)]
pub struct PendingToken {
    pub email: String,
    pub expires_at: Timestamp,
    pub session: Option<Uuid>,
}

/// A consumed one-time token.
#[derive(Clone)]
pub struct ConsumedToken {
    pub id: Uuid,
    pub subject_key: String,
    pub user_id: Option<Uuid>,
    pub email: String,
    pub email_normalized: String,
    pub return_to: Option<String>,
    pub expires_at: Timestamp,
    /// The session that requested an email change.
    pub session: Option<Uuid>,
}
impl std::fmt::Debug for ConsumedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumedToken")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

fn micros(us: i64) -> Duration {
    Duration::from_micros(u64::try_from(us).unwrap_or(0).max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_are_truncated_for_listings() {
        assert_eq!(
            truncate_ip("203.0.113.77".parse().unwrap()),
            "203.0.113.0/24"
        );
        assert_eq!(
            truncate_ip("2001:db8:abcd:12::1".parse().unwrap()),
            "2001:db8:abcd::/48"
        );
        assert_eq!(
            ip_key(Some("2001:db8:abcd:12::1".parse().unwrap())),
            ip_key(Some("2001:db8:abcd:12:ffff::9".parse().unwrap()))
        );
        assert_ne!(
            ip_key(Some("2001:db8:abcd:12::1".parse().unwrap())),
            ip_key(Some("2001:db8:abcd:13::1".parse().unwrap()))
        );
        assert_eq!(
            ip_key(Some("::ffff:203.0.113.7".parse().unwrap())),
            ip_key(Some("203.0.113.7".parse().unwrap()))
        );
    }

    #[test]
    fn user_agents_are_bounded_and_cleaned() {
        assert_eq!(Client::user_agent(None), None);
        assert_eq!(Client::user_agent(Some("a\nb")).as_deref(), Some("ab"));
        let long = "é".repeat(400);
        assert!(Client::user_agent(Some(&long)).unwrap().len() <= MAX_USER_AGENT);
    }

    #[test]
    fn audit_details_are_allowlisted() {
        let audit =
            Audit::new("session.created", Actor::User(Uuid::nil())).detail("method", "password");
        assert_eq!(audit.details, json!({"method": "password"}));
        assert_eq!(audit.target, Some(("user", Uuid::nil())));
    }
}
