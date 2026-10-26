//! `task-notifier`: the task board's reactor (AP §C.5), an application
//! component speaking protocol version 2 (declare `application_protocol =
//! 2` in the runtime manifest and a `[[reactors]]` entry naming it).
//!
//! It answers `react` only. For every `TaskCompleted` it is sent, it asks
//! the host for two effects, in the notifications block's vocabulary:
//!
//! | Index | Effect | What |
//! | --- | --- | --- |
//! | 0 | `notify` | topic `tasks.completed` on `inbox` and `email` to the user who completed the task (the event's principal), with `{"task_id"}`; omitted for an event without a user principal |
//! | next | `command` | `ArchiveTask` on the task's stream, `{}`, seven days later |
//!
//! Who completes a task is its owner or an admin (`task-domain`), and a
//! reactor sees events only, never projection state (AP3), so the user
//! notified is the one who completed it. Guests never name the dedupe key,
//! the tenant or the source event: the host derives them, so a tenant
//! task's effects stay in its tenant. Other events get no effects; a
//! `TaskCompleted` that does not decode fails `invalid_event`, which halts
//! the reactor at that event.

use merezha_domain::{
    Channel, Effect, Failure, Identity, Operation, PrincipalKind, Reaction, Recipient, Reply,
    StoredEvent,
};
use serde_json::{Map, Value, json};
use task_events::TaskEvent;

/// The notification topic, declared in `[modules.notifications.topics]`.
pub const TOPIC: &str = "tasks.completed";
/// The command the reactor schedules; the application routes it.
pub const ARCHIVE: &str = "ArchiveTask";
/// How long after completion a task is archived: seven days.
pub const ARCHIVE_DELAY_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// The effects of one event.
///
/// # Errors
///
/// `invalid_event` (and the other codes of [`TaskEvent::decode`]) for a
/// `TaskCompleted` that does not decode.
pub fn effects(event: &StoredEvent) -> Result<Vec<Effect>, Failure> {
    if event.event.tag.as_str() != "TaskCompleted" {
        return Ok(Vec::new());
    }
    let TaskEvent::Completed(completed) = TaskEvent::decode(&event.event)? else {
        return Err(Failure::permanent("invalid_event"));
    };
    let mut effects = Vec::with_capacity(2);
    if let Some(principal) = event.metadata.principal()
        && principal.kind == PrincipalKind::User
    {
        let mut data = Map::new();
        data.insert(
            "task_id".into(),
            Value::String(completed.task_id.as_str().into()),
        );
        effects.push(Effect::Notify {
            topic: Identity::new(TOPIC)?,
            recipients: vec![Recipient::User {
                id: principal.subject.clone(),
            }],
            channels: Some(vec![Channel::Inbox, Channel::Email]),
            data,
            delay_ms: None,
        });
    }
    // The host composes the stream again from the local id, under the
    // event's tenant: `{tenant}:Task/{id}` or `Task/{id}`. Neither tenants
    // nor aggregates contain `/`, so the local id follows the first one.
    let (_, local) = event
        .stream
        .as_str()
        .split_once('/')
        .ok_or_else(|| Failure::permanent("invalid_event"))?;
    effects.push(Effect::Command {
        tag: Identity::new(ARCHIVE)?,
        stream: Identity::new(local)?,
        payload: json!({}),
        expected_version: None,
        delay_ms: Some(ARCHIVE_DELAY_MS),
    });
    Ok(effects)
}

/// Answers one protocol operation.
///
/// # Errors
///
/// `unsupported_operation` for anything but `react`, and the failures of
/// [`effects`].
pub fn dispatch(operation: Operation) -> Result<Reply, Failure> {
    match operation {
        Operation::React { events, .. } => Ok(Reply::Reacted {
            reactions: events
                .iter()
                .map(|event| {
                    Ok(Reaction {
                        event_id: event.event_id.clone(),
                        effects: effects(event)?,
                    })
                })
                .collect::<Result<_, Failure>>()?,
        }),
        // `describe` never reaches a dispatcher: the SDK answers it.
        Operation::Validate { .. }
        | Operation::Decide { .. }
        | Operation::Project { .. }
        | Operation::Query { .. }
        | Operation::Describe {} => Err(Failure::permanent("unsupported_operation")),
    }
}

/// The `handle` export: one application request in, one response out, in
/// the request's version. Classified failures are successful responses;
/// see [`merezha_domain::handle`].
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
