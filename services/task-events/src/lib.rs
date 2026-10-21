//! The task board's events: the published facts that the `task-domain`
//! component records and the `task-view` component folds into its read
//! model.
//!
//! Both components are built independently; this crate is the schema they
//! share, in the role of an event registry. Every event is written with
//! [`SCHEMA_VERSION`], and decoding checks the tag, the version and the
//! payload shape, so a component never interprets a fact it does not know.

use merezha_domain::{Event, Failure, Identity};
use serde::{Deserialize, Serialize};

/// Schema version of every task board event.
pub const SCHEMA_VERSION: u16 = 1;

/// Longest accepted task title, in Unicode scalar values.
pub const MAX_TITLE_CHARS: usize = 120;

/// A task board fact. The tag is the event's [`Event::tag`]; the fields are
/// its payload. Use [`TaskEvent::encode`] and [`TaskEvent::decode`] at the
/// protocol boundary; the Serde form mirrors them for typed deciders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "tag", content = "payload")]
pub enum TaskEvent {
    /// A task was opened with a title.
    #[serde(rename = "TaskCreated")]
    Created(TaskCreated),
    /// An open task received a new title.
    #[serde(rename = "TaskRenamed")]
    Renamed(TaskRenamed),
    /// An open task was completed.
    #[serde(rename = "TaskCompleted")]
    Completed(TaskCompleted),
}

/// Payload of `TaskCreated`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCreated {
    pub task_id: Identity,
    pub title: String,
}

/// Payload of `TaskRenamed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRenamed {
    pub task_id: Identity,
    pub title: String,
}

/// Payload of `TaskCompleted`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCompleted {
    pub task_id: Identity,
}

impl TaskEvent {
    /// The event tag recorded in the store.
    pub fn tag(&self) -> &'static str {
        match self {
            Self::Created(_) => "TaskCreated",
            Self::Renamed(_) => "TaskRenamed",
            Self::Completed(_) => "TaskCompleted",
        }
    }

    /// The task the event belongs to.
    pub fn task_id(&self) -> &Identity {
        match self {
            Self::Created(event) => &event.task_id,
            Self::Renamed(event) => &event.task_id,
            Self::Completed(event) => &event.task_id,
        }
    }

    /// Encodes the event for the protocol's `decided` reply.
    ///
    /// # Errors
    ///
    /// `serialization_failed` if the payload cannot be encoded, which the
    /// payload types make impossible in practice.
    pub fn encode(&self) -> Result<Event, Failure> {
        let payload = match self {
            Self::Created(event) => serde_json::to_value(event),
            Self::Renamed(event) => serde_json::to_value(event),
            Self::Completed(event) => serde_json::to_value(event),
        }
        .map_err(|_| Failure::permanent("serialization_failed"))?;
        Ok(Event {
            tag: Identity::new(self.tag())?,
            schema_version: SCHEMA_VERSION,
            payload,
        })
    }

    /// Decodes a recorded event.
    ///
    /// # Errors
    ///
    /// `unknown_event` for a tag this schema does not define,
    /// `unsupported_schema_version` for any version but [`SCHEMA_VERSION`],
    /// and `invalid_event` when the payload does not match the tag's shape
    /// or carries an invalid title.
    pub fn decode(event: &Event) -> Result<Self, Failure> {
        let decoded = match event.tag.as_str() {
            "TaskCreated" => payload(event).map(Self::Created),
            "TaskRenamed" => payload(event).map(Self::Renamed),
            "TaskCompleted" => payload(event).map(Self::Completed),
            _ => return Err(Failure::permanent("unknown_event")),
        }?;
        if event.schema_version != SCHEMA_VERSION {
            return Err(Failure::permanent("unsupported_schema_version"));
        }
        match &decoded {
            Self::Created(TaskCreated { title, .. }) | Self::Renamed(TaskRenamed { title, .. })
                if !valid_title(title) =>
            {
                Err(Failure::permanent("invalid_event"))
            }
            _ => Ok(decoded),
        }
    }
}

fn payload<T: serde::de::DeserializeOwned>(event: &Event) -> Result<T, Failure> {
    serde_json::from_value(event.payload.clone()).map_err(|_| Failure::permanent("invalid_event"))
}

/// Whether `title` is a canonical title: 1 to [`MAX_TITLE_CHARS`]
/// characters, no surrounding whitespace and no control characters.
pub fn valid_title(title: &str) -> bool {
    !title.is_empty()
        && title.trim() == title
        && title.chars().count() <= MAX_TITLE_CHARS
        && !title.chars().any(char::is_control)
}
