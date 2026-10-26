//! The job payloads (NT §2.5). The notification row does not exist until
//! expansion, so the request travels whole in the `notify.expand` payload;
//! `secret_data` travels only in payloads flagged `contains_secret`, which
//! the relay and the jobs runtime null (R3). Delivery jobs carry the
//! delivery id, its generation and, for email and SMS, the secret data.
use merezha_platform::ports::{Address, Channel, NotificationRequest, Recipient, Timestamp, Uuid};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// One recipient as the expansion payload stores it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoredRecipient {
    User {
        id: String,
    },
    Tenant {
        id: String,
        #[serde(default)]
        roles: Option<Vec<String>>,
    },
    Application {
        roles: Vec<String>,
    },
    Email {
        address: String,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        user: Option<String>,
        #[serde(default)]
        locale: Option<String>,
    },
    Phone {
        e164: String,
        #[serde(default)]
        user: Option<String>,
        #[serde(default)]
        locale: Option<String>,
    },
}
impl std::fmt::Debug for StoredRecipient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            StoredRecipient::User { .. } => "User",
            StoredRecipient::Tenant { .. } => "Tenant",
            StoredRecipient::Application { .. } => "Application",
            StoredRecipient::Email { .. } => "Email",
            StoredRecipient::Phone { .. } => "Phone",
        })
    }
}
impl From<&Recipient> for StoredRecipient {
    fn from(recipient: &Recipient) -> Self {
        let text = |id: &merezha_domain::Identity| id.as_str().to_owned();
        match recipient {
            Recipient::User { id } => StoredRecipient::User { id: text(id) },
            Recipient::Tenant { id, roles } => StoredRecipient::Tenant {
                id: text(id),
                roles: roles.as_ref().map(|r| r.iter().map(text).collect()),
            },
            Recipient::Application { roles } => StoredRecipient::Application {
                roles: roles.iter().map(text).collect(),
            },
            Recipient::Address(Address::Email {
                address,
                name,
                user,
                locale,
            }) => StoredRecipient::Email {
                address: address.expose().to_owned(),
                name: name.clone(),
                user: user.as_ref().map(text),
                locale: locale.as_ref().map(|l| l.as_str().to_owned()),
            },
            Recipient::Address(Address::Phone { e164, user, locale }) => StoredRecipient::Phone {
                e164: e164.expose().to_owned(),
                user: user.as_ref().map(text),
                locale: locale.as_ref().map(|l| l.as_str().to_owned()),
            },
        }
    }
}

/// The `notify.expand` payload, version 1. `Debug` shows ids only.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpandPayload {
    pub v: u8,
    #[serde(with = "uuid_text")]
    pub notification_id: Uuid,
    pub source: String,
    pub dedupe_key: String,
    pub topic: String,
    pub template: String,
    /// The resolved channels, a subset of the topic's.
    pub channels: Vec<String>,
    pub recipients: Vec<StoredRecipient>,
    pub data: Map<String, Value>,
    /// Present only in a flagged payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_data: Option<Map<String, Value>>,
    #[serde(default)]
    pub locale: Option<String>,
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub not_before: Option<Timestamp>,
    #[serde(default)]
    pub expires_at: Option<Timestamp>,
    #[serde(default)]
    pub collapse_key: Option<String>,
    pub correlation_id: String,
    #[serde(default)]
    pub causation_id: Option<String>,
    pub requested_at: Timestamp,
}
impl std::fmt::Debug for ExpandPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExpandPayload")
            .field("notification_id", &self.notification_id)
            .field("source", &self.source)
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}
impl ExpandPayload {
    pub const VERSION: u8 = 1;
    /// The payload of `request` for `source`, its channels resolved.
    pub fn new(
        source: &str,
        notification_id: Uuid,
        request: &NotificationRequest,
        template: &str,
        channels: &[Channel],
        requested_at: Timestamp,
    ) -> Self {
        Self {
            v: Self::VERSION,
            notification_id,
            source: source.to_owned(),
            dedupe_key: request.dedupe_key.as_str().to_owned(),
            topic: request.topic.as_str().to_owned(),
            template: template.to_owned(),
            channels: channels.iter().map(|c| c.as_str().to_owned()).collect(),
            recipients: request.recipients.iter().map(Into::into).collect(),
            data: request.data.clone(),
            secret_data: (!request.secret_data.is_empty()).then(|| request.secret_data.clone()),
            locale: request.locale.as_ref().map(|l| l.as_str().to_owned()),
            tenant: request.tenant.as_ref().map(|t| t.as_str().to_owned()),
            not_before: request.not_before,
            expires_at: request.expires_at,
            collapse_key: request.collapse_key.clone(),
            correlation_id: request.correlation.correlation_id.as_str().to_owned(),
            causation_id: request
                .correlation
                .causation_id
                .as_ref()
                .map(|c| c.as_str().to_owned()),
            requested_at,
        }
    }
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// The `notify.deliver.<channel>` payload. `Debug` shows ids only.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverPayload {
    #[serde(with = "uuid_text")]
    pub delivery_id: Uuid,
    pub generation: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_data: Option<Map<String, Value>>,
}
impl std::fmt::Debug for DeliverPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeliverPayload")
            .field("delivery_id", &self.delivery_id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
impl DeliverPayload {
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// Ids as their hyphenated text in JSON.
pub(crate) mod uuid_text {
    use serde::{Deserialize, Deserializer, Serializer};
    use uuid::Uuid;
    pub fn serialize<S: Serializer>(id: &Uuid, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&id.hyphenated())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Uuid, D::Error> {
        let text = String::deserialize(deserializer)?;
        Uuid::try_parse(&text).map_err(serde::de::Error::custom)
    }
}
