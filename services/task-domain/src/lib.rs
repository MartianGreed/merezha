//! `task-domain`: the task board's business rules as an application
//! component.
//!
//! The host owns routing, idempotency, event storage and versions; this
//! component owns every rule. It answers two operations of the version 1
//! application protocol (`merezha_domain`):
//!
//! - `validate` decodes a command payload into its typed shape, checks the
//!   title and returns the canonical payload: surrounding whitespace is
//!   removed, so `" Write docs "` and `"Write docs"` are the same command.
//! - `decide` checks the stream's history, rehydrates the task from it and
//!   accepts or rejects the command. A rejection emits no event.
//!
//! | Command | Payload | Accepted when | Emits |
//! | --- | --- | --- | --- |
//! | `CreateTask` | `{"title":…}` | the task does not exist | `TaskCreated` |
//! | `RenameTask` | `{"title":…}` | the task is open and the title changes | `TaskRenamed` |
//! | `CompleteTask` | `{}` | the task is open | `TaskCompleted` |
//!
//! See `docs/services/README.md` for the full contract and failure codes.

use merezha_domain::{
    Decider, Event, Failure, Identity, MessageKind, Operation, Reply, StoredEvent, rehydrate,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use task_events::{TaskCompleted, TaskCreated, TaskEvent, TaskRenamed, valid_title};

/// Payload of `CreateTask`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTask {
    pub title: String,
}

/// Payload of `RenameTask`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenameTask {
    pub title: String,
}

/// Payload of `CompleteTask`: the empty object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteTask {}

/// A validated task board command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "tag", content = "payload")]
pub enum TaskCommand {
    CreateTask(CreateTask),
    RenameTask(RenameTask),
    CompleteTask(CompleteTask),
}

impl TaskCommand {
    /// Decodes and validates the payload of command `tag`, returning it in
    /// canonical form.
    ///
    /// # Errors
    ///
    /// `unknown_command` for a tag this component does not handle,
    /// `validation_failed` when the payload does not have the command's
    /// shape (unknown fields included) and `invalid_title` when a title is
    /// blank, longer than 120 characters or contains control characters.
    pub fn decode(tag: &str, payload: Value) -> Result<Self, Failure> {
        let command = match tag {
            "CreateTask" => typed(payload).map(Self::CreateTask),
            "RenameTask" => typed(payload).map(Self::RenameTask),
            "CompleteTask" => typed(payload).map(Self::CompleteTask),
            _ => return Err(Failure::permanent("unknown_command")),
        }?;
        Ok(match command {
            Self::CreateTask(CreateTask { title }) => Self::CreateTask(CreateTask {
                title: canonical_title(&title)?,
            }),
            Self::RenameTask(RenameTask { title }) => Self::RenameTask(RenameTask {
                title: canonical_title(&title)?,
            }),
            complete @ Self::CompleteTask(_) => complete,
        })
    }

    /// The canonical payload, without the tag.
    fn payload(&self) -> Result<Value, Failure> {
        match self {
            Self::CreateTask(payload) => serde_json::to_value(payload),
            Self::RenameTask(payload) => serde_json::to_value(payload),
            Self::CompleteTask(payload) => serde_json::to_value(payload),
        }
        .map_err(|_| Failure::permanent("serialization_failed"))
    }
}

fn typed<T: serde::de::DeserializeOwned>(payload: Value) -> Result<T, Failure> {
    serde_json::from_value(payload).map_err(|_| Failure::permanent("validation_failed"))
}

fn canonical_title(title: &str) -> Result<String, Failure> {
    let title = title.trim();
    if valid_title(title) {
        Ok(title.to_owned())
    } else {
        Err(Failure::permanent("invalid_title"))
    }
}

/// The state of one task, rebuilt from its stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Task {
    /// No event recorded yet.
    Absent,
    /// Created and not completed.
    Open { title: String },
    /// Completed; the task accepts no further change.
    Completed { title: String },
}

/// The decider of one task stream. `task_id` is the stream the host
/// addresses, recorded in every event so the view needs no stream naming
/// convention.
#[derive(Debug, Clone)]
pub struct TaskDecider {
    pub task_id: Identity,
}

impl Decider for TaskDecider {
    type State = Task;
    type Command = TaskCommand;
    type Event = TaskEvent;

    fn initial(&self) -> Task {
        Task::Absent
    }

    fn decide(&self, state: &Task, command: TaskCommand) -> Result<Vec<TaskEvent>, Failure> {
        let task_id = self.task_id.clone();
        let event = match (state, command) {
            (Task::Absent, TaskCommand::CreateTask(CreateTask { title })) => {
                TaskEvent::Created(TaskCreated { task_id, title })
            }
            (_, TaskCommand::CreateTask(_)) => return Err(Failure::permanent("task_exists")),
            (Task::Absent, _) => return Err(Failure::permanent("task_not_found")),
            (Task::Completed { .. }, TaskCommand::RenameTask(_)) => {
                return Err(Failure::permanent("task_completed"));
            }
            (Task::Completed { .. }, TaskCommand::CompleteTask(_)) => {
                return Err(Failure::permanent("task_already_completed"));
            }
            (Task::Open { title: current }, TaskCommand::RenameTask(RenameTask { title })) => {
                if *current == title {
                    return Err(Failure::permanent("title_unchanged"));
                }
                TaskEvent::Renamed(TaskRenamed { task_id, title })
            }
            (Task::Open { .. }, TaskCommand::CompleteTask(_)) => {
                TaskEvent::Completed(TaskCompleted { task_id })
            }
        };
        Ok(vec![event])
    }

    fn evolve(&self, state: Task, event: TaskEvent) -> Result<Task, Failure> {
        if *event.task_id() != self.task_id {
            return Err(Failure::permanent("corrupt_history"));
        }
        match (state, event) {
            (Task::Absent, TaskEvent::Created(TaskCreated { title, .. })) => {
                Ok(Task::Open { title })
            }
            (Task::Open { .. }, TaskEvent::Renamed(TaskRenamed { title, .. })) => {
                Ok(Task::Open { title })
            }
            (Task::Open { title }, TaskEvent::Completed(_)) => Ok(Task::Completed { title }),
            _ => Err(Failure::permanent("corrupt_history")),
        }
    }
}

/// Checks a stream's history and folds it into the task's state.
///
/// # Errors
///
/// `corrupt_history` when the history belongs to another stream, its
/// versions do not run 1, 2, 3… or an event cannot follow the previous
/// state; the decoding failures of [`TaskEvent::decode`] otherwise.
pub fn replay(task_id: &Identity, history: &[StoredEvent]) -> Result<Task, Failure> {
    let mut events = Vec::with_capacity(history.len());
    for (index, stored) in history.iter().enumerate() {
        if stored.stream != *task_id || stored.version != index as u64 + 1 {
            return Err(Failure::permanent("corrupt_history"));
        }
        events.push(TaskEvent::decode(&stored.event)?);
    }
    rehydrate(
        &TaskDecider {
            task_id: task_id.clone(),
        },
        events,
    )
}

/// Answers one protocol operation.
///
/// # Errors
///
/// `unsupported_operation` for `project` and `query`, `unknown_command` for
/// a query tag, and the failures of [`TaskCommand::decode`], [`replay`] and
/// [`TaskDecider::decide`].
pub fn dispatch(operation: Operation) -> Result<Reply, Failure> {
    match operation {
        Operation::Validate {
            kind: MessageKind::Command,
            tag,
            payload,
        } => Ok(Reply::Validated {
            payload: TaskCommand::decode(tag.as_str(), payload)?.payload()?,
        }),
        Operation::Validate { .. } => Err(Failure::permanent("unknown_command")),
        Operation::Decide {
            tag,
            stream,
            payload,
            history,
            ..
        } => {
            let command = TaskCommand::decode(tag.as_str(), payload)?;
            let state = replay(&stream, &history)?;
            let decider = TaskDecider { task_id: stream };
            let events = decider
                .decide(&state, command)?
                .iter()
                .map(TaskEvent::encode)
                .collect::<Result<Vec<Event>, Failure>>()?;
            Ok(Reply::Decided { events })
        }
        Operation::Project { .. } | Operation::Query { .. } => {
            Err(Failure::permanent("unsupported_operation"))
        }
    }
}

/// The `handle` export: one version 1 application request in, one response
/// out. Classified failures are successful responses; see
/// [`merezha_domain::handle`].
///
/// # Errors
///
/// Only when the response cannot be encoded within the protocol's size
/// limit, which this component's bounded replies make impossible in
/// practice.
pub fn handle(input: &str) -> Result<String, String> {
    merezha_domain::handle(input, dispatch)
}

#[cfg(target_arch = "wasm32")]
mod component {
    wit_bindgen::generate!({ path: "../../wit", world: "service" });

    struct Component;

    impl exports::merezha::service::handler::Guest for Component {
        fn handle(input: String) -> Result<String, String> {
            super::handle(&input)
        }
    }

    export!(Component);
}
