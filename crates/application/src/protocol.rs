//! Guest protocol versions on the host side (AP §C.1, §C.2): the version 1
//! downgrade, the `describe` handshake with version 2 guests and the
//! principal a trusted entry point establishes.
use crate::{Guest, GuestSession};
use merezha_domain::{
    Failure, Metadata, Operation, PROTOCOL_V1, PROTOCOL_V2, Principal, Reply, StoredEvent,
};
use std::{collections::BTreeSet, sync::Mutex, time::Instant};

/// The code of a service whose declared protocol its guest does not speak.
pub(crate) const MISMATCH: &str = "service_protocol_mismatch";

/// `operation` as a guest of `protocol` may receive it. Version 2 gets it
/// unchanged. Version 1 gets it without any version 2 addition: no
/// principal in decide or query metadata nor in any event's metadata, and
/// no projection partition, because the version 1 SDK refuses unknown
/// fields. `describe` (and any other version) fails `protocol_version`.
pub(crate) fn fit(operation: Operation, protocol: u16) -> Result<Operation, Failure> {
    let strip = |events: &mut Vec<StoredEvent>| {
        for event in events {
            event.metadata.principal = None;
        }
    };
    match (protocol, operation) {
        (PROTOCOL_V2, operation) => Ok(operation),
        (PROTOCOL_V1, Operation::Describe {}) => Err(Failure::permanent("protocol_version")),
        (PROTOCOL_V1, mut operation) => {
            match &mut operation {
                Operation::Decide {
                    history, metadata, ..
                } => {
                    metadata.principal = None;
                    strip(history);
                }
                Operation::Project {
                    events, partition, ..
                } => {
                    *partition = None;
                    strip(events);
                }
                Operation::Query { metadata, .. } => metadata.principal = None,
                Operation::Validate { .. } | Operation::Describe {} => {}
            }
            Ok(operation)
        }
        _ => Err(Failure::permanent("protocol_version")),
    }
}

/// Install `principal` in `metadata`, as the trusted entry points do, or
/// keep the call anonymous with `None`. A principal already in the
/// metadata came from the caller's body: `principal_not_trusted`. With a
/// principal, `actor` becomes its subject; a different caller-supplied
/// actor fails `actor_mismatch`.
pub(crate) fn establish(
    metadata: &mut Metadata,
    principal: Option<Principal>,
) -> Result<(), Failure> {
    if metadata.principal.is_some() {
        return Err(Failure::permanent("principal_not_trusted"));
    }
    let Some(principal) = principal else {
        return Ok(());
    };
    principal.check()?;
    if metadata
        .actor
        .as_ref()
        .is_some_and(|actor| *actor != principal.subject)
    {
        return Err(Failure::permanent("actor_mismatch"));
    }
    metadata.actor = Some(principal.subject.clone());
    metadata.principal = Some(principal);
    Ok(())
}

/// The metadata stored with events: the principal keeps `{subject, kind,
/// tenant}` and drops its roles, so audit data stays minimal.
pub(crate) fn stored(metadata: &Metadata) -> Metadata {
    let mut stored = metadata.clone();
    if let Some(principal) = &mut stored.principal {
        principal.roles.clear();
    }
    stored
}

/// The version 2 services whose `describe` answered in one generation:
/// checked at startup and again on the first call after each reload. Only
/// the newest generation seen is kept, so the set holds at most one
/// catalog's services.
#[derive(Default)]
pub(crate) struct Described(Mutex<(u64, BTreeSet<String>)>);
impl Described {
    /// Call `service` with `operation` fitted to its protocol, after
    /// checking a version 2 service's `describe` once per generation.
    pub(crate) fn call(
        &self,
        session: &dyn GuestSession,
        service: &str,
        operation: Operation,
        deadline: Instant,
    ) -> Result<Reply, Failure> {
        let protocol = session.protocol(service);
        if protocol == PROTOCOL_V2 {
            self.ensure(session, service, deadline)?;
        }
        session.invoke_until(service, fit(operation, protocol)?, deadline)
    }
    fn ensure(
        &self,
        session: &dyn GuestSession,
        service: &str,
        deadline: Instant,
    ) -> Result<(), Failure> {
        let generation = session.generation();
        if self.known(generation, service)? {
            return Ok(());
        }
        describe(session, service, deadline)?;
        let mut cache = self.0.lock().map_err(|_| Failure::transient("internal"))?;
        if cache.0 < generation {
            *cache = (generation, BTreeSet::new());
        }
        if cache.0 == generation {
            cache.1.insert(service.to_owned());
        }
        Ok(())
    }
    fn known(&self, generation: u64, service: &str) -> Result<bool, Failure> {
        let cache = self.0.lock().map_err(|_| Failure::transient("internal"))?;
        Ok(cache.0 == generation && cache.1.contains(service))
    }
    /// The startup check: every service the guest declares as version 2
    /// answers `describe`, else `service_protocol_mismatch`.
    pub(crate) fn check_all(&self, guest: &dyn Guest, deadline: Instant) -> Result<(), Failure> {
        let session = guest.session()?;
        for service in guest.services()? {
            if session.protocol(&service) == PROTOCOL_V2 {
                self.ensure(session.as_ref(), &service, deadline)?;
            }
        }
        Ok(())
    }
}

/// One `describe` call. The guest must answer `described` with protocol 2.
/// A version 1 guest answers `protocol_version` (or a version 1 response,
/// the same code): any permanent failure or other reply is
/// `service_protocol_mismatch`. Transient failures (timeout, overload)
/// stay what they are and are not cached.
fn describe(session: &dyn GuestSession, service: &str, deadline: Instant) -> Result<(), Failure> {
    match session.invoke_until(service, Operation::Describe {}, deadline) {
        Ok(Reply::Described {
            protocol: PROTOCOL_V2,
        }) => Ok(()),
        Err(failure) if failure.classification != merezha_domain::Classification::Permanent => {
            Err(failure)
        }
        _ => Err(Failure::permanent(MISMATCH)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use merezha_domain::{Identity, PrincipalKind};
    use serde_json::json;

    fn id(value: &str) -> Identity {
        Identity::new(value).unwrap()
    }
    fn principal() -> Principal {
        Principal::new(
            id("usr_1"),
            PrincipalKind::User,
            Some(id("org_1")),
            vec![id("admin")],
        )
        .unwrap()
    }
    fn metadata() -> Metadata {
        serde_json::from_value(json!({"message_id":"m","correlation_id":"c"})).unwrap()
    }

    #[test]
    fn establish_trusts_only_the_host() {
        let mut anonymous = metadata();
        establish(&mut anonymous, None).unwrap();
        assert_eq!(anonymous, metadata());
        let mut trusted = metadata();
        establish(&mut trusted, Some(principal())).unwrap();
        assert_eq!(trusted.actor, Some(id("usr_1")));
        assert_eq!(trusted.principal, Some(principal()));
        // Injected through the body, for either entry point.
        assert_eq!(
            establish(&mut trusted.clone(), None),
            Err(Failure::permanent("principal_not_trusted"))
        );
        assert_eq!(
            establish(&mut trusted, Some(principal())),
            Err(Failure::permanent("principal_not_trusted"))
        );
        let mut other = metadata();
        other.actor = Some(id("usr_2"));
        assert_eq!(
            establish(&mut other, Some(principal())),
            Err(Failure::permanent("actor_mismatch"))
        );
        let mut same = metadata();
        same.actor = Some(id("usr_1"));
        establish(&mut same, Some(principal())).unwrap();
        let mut unsorted = principal();
        unsorted.roles = vec![id("b"), id("a")];
        assert_eq!(
            establish(&mut metadata(), Some(unsorted)),
            Err(Failure::permanent("invalid_principal"))
        );
    }

    #[test]
    fn stored_metadata_drops_roles_only() {
        let mut trusted = metadata();
        establish(&mut trusted, Some(principal())).unwrap();
        let kept = stored(&trusted);
        assert_eq!(
            serde_json::to_value(&kept).unwrap()["principal"],
            json!({"subject":"usr_1","kind":"user","tenant":"org_1"})
        );
        assert_eq!(stored(&metadata()), metadata());
    }

    #[test]
    fn version_1_operations_lose_every_version_2_addition() {
        let mut trusted = metadata();
        establish(&mut trusted, Some(principal())).unwrap();
        let event: StoredEvent = serde_json::from_value(json!({
            "event_id":"event/1","stream":"A/1","version":1,"position":1,
            "event":{"tag":"T","schema_version":1,"payload":{}},
            "metadata": stored(&trusted),"accepted_unix_ms":1
        }))
        .unwrap();
        let operations = [
            Operation::Decide {
                tag: id("T"),
                stream: id("A/1"),
                payload: json!({}),
                history: vec![event.clone()],
                metadata: trusted.clone(),
            },
            Operation::Project {
                projection: id("p"),
                state: json!({}),
                events: vec![event],
                live: true,
                partition: Some(id("org_1")),
            },
            Operation::Query {
                tag: id("Q"),
                payload: json!({}),
                state: json!({}),
                metadata: trusted,
            },
        ];
        for operation in operations {
            assert!(!operation.fits(PROTOCOL_V1));
            let v2 = fit(operation.clone(), PROTOCOL_V2).unwrap();
            assert_eq!(
                serde_json::to_value(&v2).unwrap(),
                serde_json::to_value(&operation).unwrap()
            );
            let v1 = fit(operation, PROTOCOL_V1).unwrap();
            assert!(v1.fits(PROTOCOL_V1));
            assert!(!serde_json::to_string(&v1).unwrap().contains("principal"));
        }
        assert_eq!(
            fit(Operation::Describe {}, PROTOCOL_V1).unwrap_err(),
            Failure::permanent("protocol_version")
        );
        assert!(fit(Operation::Describe {}, 3).is_err());
    }
}
