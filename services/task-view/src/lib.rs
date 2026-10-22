//! `task-view`: the task board's read model and queries as an application
//! component.
//!
//! The host keeps the read model's state and checkpoint and passes them in;
//! this component is a pure function of its input. It answers three
//! operations of the version 1 application protocol (`merezha_domain`):
//!
//! - `project` folds a batch of recorded events into the state of
//!   projection [`PROJECTION`]. Events the state already reflects are
//!   skipped, so a redelivered batch changes nothing.
//! - `validate` decodes a query payload into its typed, canonical shape.
//! - `query` answers `GetTask`, `ListTasks` or `TaskSummary` from the state.
//!   Queries only read; the protocol gives them no way to record events.
//!
//! The default build is the correct revision. The `faulty` feature builds
//! a revision that fails every batch containing `TaskRenamed`; see
//! `docs/services/README.md`.

use std::collections::BTreeMap;

use merezha_domain::{Failure, Identity, MessageKind, Operation, Reply, StoredEvent};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use task_events::{TaskCompleted, TaskCreated, TaskEvent, TaskRenamed};

/// The only projection this component maintains.
pub const PROJECTION: &str = "tasks";

/// Default and largest page size of `ListTasks`.
pub const DEFAULT_LIMIT: u16 = 50;
pub const MAX_LIMIT: u16 = 100;

/// Whether this build is the faulty revision.
pub const FAULTY: bool = cfg!(feature = "faulty");

/// Lifecycle of a task as the read model shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    Completed,
}

/// One task in the read model. `version` is the last stream version
/// applied, which makes redelivery detectable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Row {
    pub title: String,
    pub status: Status,
    pub version: u64,
}

/// The read model: every task by id. Its initial value is
/// `{"tasks":{}}`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Board {
    pub tasks: BTreeMap<Identity, Row>,
}

impl Board {
    /// Applies one recorded event.
    ///
    /// # Errors
    ///
    /// `projection_gap` when the event does not directly follow the task's
    /// last applied version or cannot follow its status; in the faulty
    /// build, transient `projection_failed` for `TaskRenamed`.
    pub fn apply(&mut self, version: u64, event: TaskEvent) -> Result<(), Failure> {
        let gap = || Failure::permanent("projection_gap");
        let row = self.tasks.get(event.task_id());
        if row.is_some_and(|row| row.version >= version) {
            return Ok(());
        }
        let expected = row.map_or(1, |row| row.version + 1);
        if version != expected {
            return Err(gap());
        }
        match event {
            // Only the first event of a stream creates its task.
            TaskEvent::Created(_) if row.is_some() => return Err(gap()),
            TaskEvent::Created(TaskCreated { task_id, title }) => {
                let row = Row {
                    title,
                    status: Status::Open,
                    version,
                };
                self.tasks.insert(task_id, row);
            }
            TaskEvent::Renamed(_) if FAULTY => {
                return Err(Failure::transient("projection_failed"));
            }
            TaskEvent::Renamed(TaskRenamed { task_id, title }) => {
                let row = self.open_row(&task_id).ok_or_else(gap)?;
                row.title = title;
                row.version = version;
            }
            TaskEvent::Completed(TaskCompleted { task_id }) => {
                let row = self.open_row(&task_id).ok_or_else(gap)?;
                row.status = Status::Completed;
                row.version = version;
            }
        }
        Ok(())
    }

    fn open_row(&mut self, task_id: &Identity) -> Option<&mut Row> {
        self.tasks
            .get_mut(task_id)
            .filter(|row| row.status == Status::Open)
    }

    /// Folds a batch in order. The batch applies completely or not at all:
    /// on failure the caller keeps its previous state.
    ///
    /// # Errors
    ///
    /// The decoding failures of [`TaskEvent::decode`] and the failures of
    /// [`Board::apply`].
    pub fn project(mut self, events: &[StoredEvent]) -> Result<Self, Failure> {
        for stored in events {
            self.apply(stored.version, TaskEvent::decode(&stored.event)?)?;
        }
        Ok(self)
    }

    fn task(&self, task_id: &Identity) -> Option<TaskView> {
        self.tasks
            .get(task_id)
            .map(|row| TaskView::new(task_id, row))
    }
}

/// Payload of `GetTask`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetTask {
    pub task_id: Identity,
}

/// Payload of `ListTasks`: tasks in id order after `after`, optionally only
/// those with `status`, at most `limit` (default 50, 1 to 100).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListTasks {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Identity>,
    #[serde(default = "default_limit")]
    pub limit: u16,
}

fn default_limit() -> u16 {
    DEFAULT_LIMIT
}

/// Payload of `TaskSummary`: the empty object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSummary {}

/// A validated task board query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskQuery {
    Get(GetTask),
    List(ListTasks),
    Summary(TaskSummary),
}

impl TaskQuery {
    /// Decodes and validates the payload of query `tag`.
    ///
    /// # Errors
    ///
    /// `unknown_query` for a tag this component does not answer,
    /// `validation_failed` when the payload does not have the query's shape
    /// and `invalid_limit` for a `ListTasks` limit outside 1 to 100.
    pub fn decode(tag: &str, payload: Value) -> Result<Self, Failure> {
        let query = match tag {
            "GetTask" => typed(payload).map(Self::Get),
            "ListTasks" => typed(payload).map(Self::List),
            "TaskSummary" => typed(payload).map(Self::Summary),
            _ => return Err(Failure::permanent("unknown_query")),
        }?;
        if let Self::List(ListTasks { limit, .. }) = &query
            && !(1..=MAX_LIMIT).contains(limit)
        {
            return Err(Failure::permanent("invalid_limit"));
        }
        Ok(query)
    }

    fn payload(&self) -> Result<Value, Failure> {
        match self {
            Self::Get(payload) => serde_json::to_value(payload),
            Self::List(payload) => serde_json::to_value(payload),
            Self::Summary(payload) => serde_json::to_value(payload),
        }
        .map_err(|_| Failure::permanent("serialization_failed"))
    }

    /// Answers the query from `board`.
    ///
    /// # Errors
    ///
    /// `task_not_found` when `GetTask` names a task the read model does not
    /// hold.
    pub fn answer(&self, board: &Board) -> Result<Value, Failure> {
        let value = match self {
            Self::Get(GetTask { task_id }) => serde_json::to_value(
                board
                    .task(task_id)
                    .ok_or_else(|| Failure::permanent("task_not_found"))?,
            ),
            Self::List(list) => serde_json::to_value(page(board, list)),
            Self::Summary(_) => {
                let completed = board
                    .tasks
                    .values()
                    .filter(|row| row.status == Status::Completed)
                    .count();
                serde_json::to_value(Summary {
                    total: board.tasks.len(),
                    open: board.tasks.len() - completed,
                    completed,
                })
            }
        };
        value.map_err(|_| Failure::permanent("serialization_failed"))
    }
}

fn typed<T: serde::de::DeserializeOwned>(payload: Value) -> Result<T, Failure> {
    serde_json::from_value(payload).map_err(|_| Failure::permanent("validation_failed"))
}

/// One task in a query answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskView {
    pub task_id: Identity,
    pub title: String,
    pub status: Status,
    pub version: u64,
}

impl TaskView {
    fn new(task_id: &Identity, row: &Row) -> Self {
        Self {
            task_id: task_id.clone(),
            title: row.title.clone(),
            status: row.status,
            version: row.version,
        }
    }
}

/// Answer of `ListTasks`. `next` is the `after` value for the following
/// page, present only when more tasks match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Page {
    pub tasks: Vec<TaskView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<Identity>,
}

/// Answer of `TaskSummary`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub total: usize,
    pub open: usize,
    pub completed: usize,
}

fn page(board: &Board, list: &ListTasks) -> Page {
    let mut matching = board
        .tasks
        .iter()
        .filter(|(task_id, _)| list.after.as_ref().is_none_or(|after| *task_id > after))
        .filter(|(_, row)| list.status.is_none_or(|status| row.status == status))
        .map(|(task_id, row)| TaskView::new(task_id, row));
    let tasks: Vec<TaskView> = matching.by_ref().take(usize::from(list.limit)).collect();
    let next = match (matching.next(), tasks.last()) {
        (Some(_), Some(last)) => Some(last.task_id.clone()),
        _ => None,
    };
    Page { tasks, next }
}

fn board(state: Value) -> Result<Board, Failure> {
    serde_json::from_value(state).map_err(|_| Failure::permanent("invalid_state"))
}

fn encode_board(board: &Board) -> Result<Value, Failure> {
    serde_json::to_value(board).map_err(|_| Failure::permanent("serialization_failed"))
}

/// Answers one protocol operation.
///
/// # Errors
///
/// `unsupported_operation` for `decide`, `unknown_projection` for any
/// projection but [`PROJECTION`], `unknown_query` for a command tag,
/// `invalid_state` when the state is not a read model of this component,
/// and the failures of [`Board::project`], [`TaskQuery::decode`] and
/// [`TaskQuery::answer`].
pub fn dispatch(operation: Operation) -> Result<Reply, Failure> {
    match operation {
        Operation::Project {
            projection,
            state,
            events,
            // The read model has no side effects, so replay during a
            // rebuild (`live: false`) is handled exactly like live delivery.
            live: _,
        } => {
            if projection.as_str() != PROJECTION {
                return Err(Failure::permanent("unknown_projection"));
            }
            let board = board(state)?.project(&events)?;
            Ok(Reply::Projected {
                state: encode_board(&board)?,
            })
        }
        Operation::Validate {
            kind: MessageKind::Query,
            tag,
            payload,
        } => Ok(Reply::Validated {
            payload: TaskQuery::decode(tag.as_str(), payload)?.payload()?,
        }),
        Operation::Validate { .. } => Err(Failure::permanent("unknown_query")),
        Operation::Query {
            tag,
            payload,
            state,
            ..
        } => {
            let query = TaskQuery::decode(tag.as_str(), payload)?;
            Ok(Reply::Queried {
                value: query.answer(&board(state)?)?,
            })
        }
        Operation::Decide { .. } => Err(Failure::permanent("unsupported_operation")),
    }
}

/// The `handle` export: one version 1 application request in, one response
/// out. Classified failures are successful responses; see
/// [`merezha_domain::handle`].
///
/// # Errors
///
/// Only when the response exceeds the protocol's size limit; the host's
/// state limit keeps the read model well below it.
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
