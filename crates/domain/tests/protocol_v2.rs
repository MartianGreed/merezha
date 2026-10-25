//! Protocol version 2 (AP §C.2, §C.3): golden JSON for every new type,
//! the automatic `describe`, version echo and the version 1 refusals.
use merezha_domain::{
    Event, Failure, Identity, LATEST_PROTOCOL, MAX_PRINCIPAL_ROLES, Metadata, Operation,
    PROTOCOL_V1, PROTOCOL_V2, Principal, PrincipalKind, Reply, Request, Response, StoredEvent,
    handle,
};
use serde_json::{Value, json};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn principal() -> Principal {
    Principal::new(
        id("usr_01JB2X6Q9F3W"),
        PrincipalKind::User,
        Some(id("org_01JB2X7")),
        vec![id("member"), id("admin"), id("member")],
    )
    .unwrap()
}
fn metadata(principal: Option<Principal>) -> Metadata {
    Metadata {
        message_id: id("m-1"),
        correlation_id: id("c-1"),
        causation_id: None,
        actor: Some(id("usr_01JB2X6Q9F3W")),
        principal,
    }
}
fn stored(principal: Option<Principal>) -> StoredEvent {
    StoredEvent {
        event_id: id("event/1"),
        stream: id("Task/t-1"),
        version: 1,
        position: 1,
        event: Event {
            tag: id("TaskCreated"),
            schema_version: 2,
            payload: json!({"task_id":"t-1","title":"Write docs"}),
        },
        metadata: metadata(principal),
        accepted_unix_ms: 1_700_000_000_000,
    }
}
fn echo(operation: Operation) -> Result<Reply, Failure> {
    match operation {
        Operation::Query { metadata, .. } => Ok(Reply::Queried {
            value: serde_json::to_value(metadata.principal()).unwrap(),
        }),
        _ => Err(Failure::permanent("unsupported_operation")),
    }
}

#[test]
fn principal_golden_json_sorts_roles_and_omits_absent_fields() {
    assert_eq!(
        serde_json::to_string(&principal()).unwrap(),
        r#"{"subject":"usr_01JB2X6Q9F3W","kind":"user","tenant":"org_01JB2X7","roles":["admin","member"]}"#
    );
    // Stored metadata keeps no roles; a principal without tenant omits it.
    let system = Principal::new(
        id("sys_reactor_notifier"),
        PrincipalKind::System,
        None,
        vec![],
    )
    .unwrap();
    assert_eq!(
        serde_json::to_string(&system).unwrap(),
        r#"{"subject":"sys_reactor_notifier","kind":"system"}"#
    );
    let key = Principal::new(id("key_1"), PrincipalKind::ApiKey, None, vec![]).unwrap();
    assert_eq!(
        serde_json::to_value(&key).unwrap()["kind"],
        json!("api_key")
    );
    assert_eq!(
        serde_json::to_string(&metadata(Some(system))).unwrap(),
        r#"{"message_id":"m-1","correlation_id":"c-1","causation_id":null,"actor":"usr_01JB2X6Q9F3W","principal":{"subject":"sys_reactor_notifier","kind":"system"}}"#
    );
    // Decoding normalizes the role order and rejects unknown fields.
    let decoded: Principal =
        serde_json::from_value(json!({"subject":"usr_1","kind":"user","roles":["b","a","b"]}))
            .unwrap();
    assert_eq!(decoded.roles, vec![id("a"), id("b")]);
    assert!(
        serde_json::from_value::<Principal>(json!({"subject":"usr_1","kind":"user","session":"s"}))
            .is_err()
    );
}

#[test]
fn principal_bounds_and_helpers() {
    let many: Vec<Identity> = (0..=MAX_PRINCIPAL_ROLES)
        .map(|i| id(&format!("r{i:02}")))
        .collect();
    assert_eq!(
        Principal::new(id("usr_1"), PrincipalKind::User, None, many.clone()),
        Err(Failure::permanent("invalid_principal"))
    );
    assert!(
        Principal::new(
            id("usr_1"),
            PrincipalKind::User,
            None,
            many[..MAX_PRINCIPAL_ROLES].to_vec()
        )
        .is_ok()
    );
    assert_eq!(
        Principal::new(
            id("usr_1"),
            PrincipalKind::User,
            None,
            vec![id(&"r".repeat(65))]
        ),
        Err(Failure::permanent("invalid_principal"))
    );
    assert!(
        serde_json::from_value::<Principal>(json!({"subject":"usr_1","kind":"user","roles": many}))
            .is_err()
    );
    let mut unsorted = principal();
    unsorted.roles.reverse();
    assert_eq!(
        unsorted.check(),
        Err(Failure::permanent("invalid_principal"))
    );
    let p = principal();
    assert!(p.has_role("admin") && !p.has_role("owner"));
    assert_eq!(p.require_tenant(), Ok(&id("org_01JB2X7")));
    let anonymous = metadata(None);
    assert_eq!(
        anonymous.require_subject(),
        Err(Failure::permanent("unauthenticated"))
    );
    assert_eq!(
        metadata(Some(p)).require_subject(),
        Ok(&id("usr_01JB2X6Q9F3W"))
    );
    let no_tenant = Principal::new(id("usr_1"), PrincipalKind::User, None, vec![]).unwrap();
    assert_eq!(
        no_tenant.require_tenant(),
        Err(Failure::permanent("tenant_required"))
    );
}

#[test]
fn version_2_operations_golden_json() {
    assert_eq!(
        serde_json::to_string(&Request::with_version(PROTOCOL_V2, Operation::Describe {})).unwrap(),
        r#"{"version":2,"body":{"op":"describe"}}"#
    );
    let project = Operation::Project {
        projection: id("tasks"),
        state: json!({}),
        events: vec![],
        live: true,
        partition: Some(id("org_1")),
    };
    assert_eq!(
        serde_json::to_string(&Request::with_version(PROTOCOL_V2, project)).unwrap(),
        r#"{"version":2,"body":{"op":"project","projection":"tasks","state":{},"events":[],"live":true,"partition":"org_1"}}"#
    );
    assert_eq!(
        Response::encode(Ok(Reply::Described { protocol: 2 })).unwrap(),
        r#"{"version":1,"outcome":{"status":"ok","reply":{"kind":"described","protocol":2}}}"#
    );
    assert_eq!(
        serde_json::to_string(&Response::new_with_version(
            PROTOCOL_V2,
            Ok(Reply::Described { protocol: 2 })
        ))
        .unwrap(),
        r#"{"version":2,"outcome":{"status":"ok","reply":{"kind":"described","protocol":2}}}"#
    );
    let decide = Operation::Decide {
        tag: id("CreateTask"),
        stream: id("Task/t-1"),
        payload: json!({}),
        history: vec![stored(Some(
            Principal::new(
                id("usr_01JB2X6Q9F3W"),
                PrincipalKind::User,
                Some(id("org_01JB2X7")),
                vec![],
            )
            .unwrap(),
        ))],
        metadata: metadata(Some(principal())),
    };
    let text = serde_json::to_string(&Request::with_version(PROTOCOL_V2, decide)).unwrap();
    assert!(text.contains(r#""metadata":{"message_id":"m-1","correlation_id":"c-1","causation_id":null,"actor":"usr_01JB2X6Q9F3W","principal":{"subject":"usr_01JB2X6Q9F3W","kind":"user","tenant":"org_01JB2X7","roles":["admin","member"]}}}}"#));
    assert!(text.contains(r#""principal":{"subject":"usr_01JB2X6Q9F3W","kind":"user","tenant":"org_01JB2X7"}},"accepted_unix_ms""#));
    assert_eq!(Request::decode(&text).unwrap().version, PROTOCOL_V2);
}

#[test]
fn describe_is_answered_by_the_sdk_in_the_request_version() {
    let unreachable = |_: Operation| -> Result<Reply, Failure> { panic!("dispatch called") };
    let output = handle(r#"{"version":2,"body":{"op":"describe"}}"#, unreachable).unwrap();
    assert_eq!(
        output,
        r#"{"version":2,"outcome":{"status":"ok","reply":{"kind":"described","protocol":2}}}"#
    );
    assert_eq!(
        Response::decode_version(&output, PROTOCOL_V2),
        Ok(Reply::Described {
            protocol: LATEST_PROTOCOL
        })
    );
    // Describe is a version 2 operation; extra fields are refused.
    let v1 = handle(r#"{"version":1,"body":{"op":"describe"}}"#, unreachable).unwrap();
    assert_eq!(
        Response::decode(&v1),
        Err(Failure::permanent("protocol_version"))
    );
    let extra = handle(
        r#"{"version":2,"body":{"op":"describe","x":1}}"#,
        unreachable,
    )
    .unwrap();
    assert_eq!(
        Response::decode_version(&extra, PROTOCOL_V2),
        Err(Failure::permanent("validation_failed"))
    );
}

#[test]
fn handle_echoes_the_version_and_passes_the_principal() {
    let query = |version: u16, principal: Option<Principal>| {
        Request::with_version(
            version,
            Operation::Query {
                tag: id("Who"),
                payload: json!({}),
                state: json!({}),
                metadata: metadata(principal),
            },
        )
    };
    let input = serde_json::to_string(&query(PROTOCOL_V2, Some(principal()))).unwrap();
    let output = handle(&input, echo).unwrap();
    assert!(output.starts_with(r#"{"version":2,"#));
    assert_eq!(
        Response::decode_version(&output, PROTOCOL_V2),
        Ok(Reply::Queried {
            value: serde_json::to_value(principal()).unwrap()
        })
    );
    // A version 1 response does not satisfy a version 2 host, and back.
    assert_eq!(
        Response::decode(&output),
        Err(Failure::permanent("protocol_version"))
    );
    let input = serde_json::to_string(&query(PROTOCOL_V1, None)).unwrap();
    let output = handle(&input, echo).unwrap();
    assert_eq!(
        Response::decode(&output),
        Ok(Reply::Queried { value: Value::Null })
    );
    assert_eq!(
        Response::decode_version(&output, PROTOCOL_V2),
        Err(Failure::permanent("protocol_version"))
    );
    // A malformed version 2 request fails in version 2.
    let bad = handle(r#"{"version":2,"body":{"op":"nope"}}"#, echo).unwrap();
    assert_eq!(
        Response::decode_version(&bad, PROTOCOL_V2),
        Err(Failure::permanent("validation_failed"))
    );
    let unknown = handle(r#"{"version":3,"body":{"op":"describe"}}"#, echo).unwrap();
    assert_eq!(
        Response::decode(&unknown),
        Err(Failure::permanent("protocol_version"))
    );
}

#[test]
fn version_1_refuses_every_version_2_addition() {
    let refused = |operation: Operation| {
        let text = serde_json::to_string(&Request::new(operation.clone())).unwrap();
        assert_eq!(
            Request::decode(&text).unwrap_err(),
            Failure::permanent("protocol_version")
        );
        assert!(!operation.fits(PROTOCOL_V1) && operation.fits(PROTOCOL_V2));
    };
    refused(Operation::Describe {});
    refused(Operation::Project {
        projection: id("tasks"),
        state: json!({}),
        events: vec![],
        live: true,
        partition: Some(id("org_1")),
    });
    refused(Operation::Project {
        projection: id("tasks"),
        state: json!({}),
        events: vec![stored(Some(principal()))],
        live: true,
        partition: None,
    });
    refused(Operation::Query {
        tag: id("Who"),
        payload: json!({}),
        state: json!({}),
        metadata: metadata(Some(principal())),
    });
    refused(Operation::Decide {
        tag: id("CreateTask"),
        stream: id("Task/t-1"),
        payload: json!({}),
        history: vec![stored(Some(principal()))],
        metadata: metadata(None),
    });
    refused(Operation::Decide {
        tag: id("CreateTask"),
        stream: id("Task/t-1"),
        payload: json!({}),
        history: vec![],
        metadata: metadata(Some(principal())),
    });
    assert!(!Operation::Describe {}.fits(3));
}
