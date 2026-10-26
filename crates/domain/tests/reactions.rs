//! Reactions (AP §C.2, C3): golden JSON for `react`, `reacted` and every
//! effect type in the notifications block's vocabulary (R4, NT §3.3), the
//! version 1 refusal of `react`, and strict decoding of guest replies.
use merezha_domain::{
    Address, Channel, Effect, EmailAddress, Event, Failure, Identity, Metadata, Operation,
    PROTOCOL_V1, PROTOCOL_V2, PhoneAddress, Reaction, Recipient, Reply, Request, Response,
    StoredEvent, handle,
};
use serde_json::{Value, json};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn stored(position: u64) -> StoredEvent {
    StoredEvent {
        event_id: id(&format!("event/{position}")),
        stream: id("org_1:Task/t-1"),
        version: position,
        position,
        event: Event {
            tag: id("TaskCompleted"),
            schema_version: 1,
            payload: json!({"task_id": "t-1"}),
        },
        metadata: Metadata {
            message_id: id("m-1"),
            correlation_id: id("c-1"),
            causation_id: None,
            actor: None,
            principal: None,
        },
        accepted_unix_ms: 1_700_000_000_000,
    }
}

/// The reply of AP §C.2, verbatim.
const REACTED: &str = r#"{"version":2,"outcome":{"status":"ok","reply":{"kind":"reacted","reactions":[{"event_id":"event/41","effects":[{"effect":"notify","topic":"tasks.completed","recipients":[{"user":{"id":"usr_01JB2X6Q9F3W"}}],"channels":["inbox","email"],"data":{"title":"Write docs"}},{"effect":"webhook","event_type":"tasks.task_completed","data":{"task_id":"t-1"}},{"effect":"command","tag":"ArchiveTask","stream":"t-1","payload":{},"delay_ms":604800000}]},{"event_id":"event/42","effects":[]}]}}}"#;

#[test]
fn the_design_reply_decodes_and_encodes_byte_for_byte() {
    let reply = Response::decode_version(REACTED, PROTOCOL_V2).unwrap();
    let Reply::Reacted { reactions } = &reply else {
        panic!("reacted: {reply:?}");
    };
    assert_eq!(
        reactions[0].effects[0],
        Effect::Notify {
            topic: id("tasks.completed"),
            recipients: vec![Recipient::User {
                id: id("usr_01JB2X6Q9F3W")
            }],
            channels: Some(vec![Channel::Inbox, Channel::Email]),
            data: json!({"title": "Write docs"}).as_object().unwrap().clone(),
            delay_ms: None,
        }
    );
    assert_eq!(
        reactions[0]
            .effects
            .iter()
            .map(Effect::kind)
            .collect::<Vec<_>>(),
        ["notify", "webhook", "command"]
    );
    assert_eq!(
        reactions[0].effects[2],
        Effect::Command {
            tag: id("ArchiveTask"),
            stream: id("t-1"),
            payload: json!({}),
            expected_version: None,
            delay_ms: Some(604_800_000),
        }
    );
    assert_eq!(reactions[1].effects, vec![]);
    let encoded = merezha_domain::encode(&Response::new_with_version(PROTOCOL_V2, Ok(reply)));
    assert_eq!(encoded.unwrap(), REACTED);
}

#[test]
fn every_recipient_form_has_golden_json() {
    let recipients = vec![
        Recipient::User { id: id("usr_1") },
        Recipient::Tenant {
            id: id("org_1"),
            roles: Some(vec![id("admin")]),
        },
        Recipient::Tenant {
            id: id("org_1"),
            roles: None,
        },
        Recipient::Application {
            roles: vec![id("support")],
        },
        Recipient::Address(Address::Email(EmailAddress {
            email: "ada@example.com".into(),
        })),
        Recipient::Address(Address::Phone(PhoneAddress {
            phone: "+33612345678".into(),
        })),
    ];
    let text = serde_json::to_string(&recipients).unwrap();
    assert_eq!(
        text,
        r#"[{"user":{"id":"usr_1"}},{"tenant":{"id":"org_1","roles":["admin"]}},{"tenant":{"id":"org_1"}},{"application":{"roles":["support"]}},{"address":{"email":"ada@example.com"}},{"address":{"phone":"+33612345678"}}]"#
    );
    let back: Vec<Recipient> = serde_json::from_str(&text).unwrap();
    assert_eq!(back, recipients);
    // Addresses are personal data.
    let printed = format!("{recipients:?}");
    assert!(!printed.contains("ada@") && !printed.contains("612345678"));
}

#[test]
fn react_requests_are_version_2_only() {
    let operation = Operation::React {
        reactor: id("task-notifier"),
        events: vec![stored(41)],
    };
    assert!(operation.fits(PROTOCOL_V2) && !operation.fits(PROTOCOL_V1));
    let text =
        serde_json::to_string(&Request::with_version(PROTOCOL_V2, operation.clone())).unwrap();
    assert!(text.starts_with(
        r#"{"version":2,"body":{"op":"react","reactor":"task-notifier","events":[{"event_id":"event/41""#
    ));
    assert!(Request::decode(&text).is_ok());
    let v1 = serde_json::to_string(&Request::new(operation)).unwrap();
    assert_eq!(
        Request::decode(&v1).unwrap_err(),
        Failure::permanent("protocol_version")
    );
    // A guest built on the SDK answers it through its dispatcher.
    let answered = handle(&text, |operation| match operation {
        Operation::React { events, .. } => Ok(Reply::Reacted {
            reactions: events
                .iter()
                .map(|e| Reaction {
                    event_id: e.event_id.clone(),
                    effects: vec![],
                })
                .collect(),
        }),
        _ => Err(Failure::permanent("unsupported_operation")),
    })
    .unwrap();
    assert_eq!(
        answered,
        r#"{"version":2,"outcome":{"status":"ok","reply":{"kind":"reacted","reactions":[{"event_id":"event/41","effects":[]}]}}}"#
    );
}

#[test]
fn effects_decode_strictly() {
    let decode = |effect: Value| serde_json::from_value::<Effect>(effect);
    // Unknown fields, unknown types, a guest-supplied dedupe key or tenant.
    for bad in [
        json!({"effect":"notify","topic":"t.x","recipients":[],"dedupe_key":"k"}),
        json!({"effect":"webhook","event_type":"t.x_done","data":{},"url":"https://x"}),
        json!({"effect":"command","tag":"A","stream":"s","payload":{},"tenant":"org_2"}),
        json!({"effect":"email","to":"x"}),
        json!({"effect":"notify","topic":"t.x","recipients":[{"user":{"id":"u","x":1}}]}),
        json!({"effect":"notify","topic":"t.x","recipients":[{"address":{"email":"a@b","phone":"+331"}}]}),
        json!({"effect":"notify","topic":"t.x","recipients":[{"group":{"id":"g"}}]}),
        json!({"effect":"notify","topic":"t.x","recipients":[],"channels":["fax"]}),
        json!({"effect":"command","tag":"A","stream":"bad space","payload":{}}),
    ] {
        assert!(decode(bad.clone()).is_err(), "{bad}");
    }
    let minimal =
        decode(json!({"effect":"notify","topic":"t.x","recipients":[{"user":{"id":"u"}}]}))
            .unwrap();
    assert_eq!(
        serde_json::to_value(&minimal).unwrap(),
        json!({"effect":"notify","topic":"t.x","recipients":[{"user":{"id":"u"}}],"data":{}})
    );
    // A reaction carries nothing but its event id and effects.
    assert!(
        serde_json::from_value::<Reaction>(json!({"event_id":"event/1","effects":[],"extra":1}))
            .is_err()
    );
}
