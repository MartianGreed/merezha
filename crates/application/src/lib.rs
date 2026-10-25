//! Bounded application orchestration with memory, owned SQLite or (feature
//! `postgres`) shared PostgreSQL storage. The host follows the versioned
//! guest contract in `merezha-domain`.
mod application;
pub mod config;
mod durable;
mod instrument;
#[cfg(feature = "postgres")]
pub mod postgres;
mod protocol;
mod sqlite;
pub mod status;
pub mod store;
pub use application::{Application, ProjectionState, QueryResult};
pub use instrument::SCOPE;

use merezha_domain::{Failure, MessageKind, Metadata, Operation, Reply};
use serde_json::Value;

/// A pinned guest generation for a complete dispatch. Implementations must
/// bound execution, input and output and create a fresh guest per call.
///
/// The application fits every operation to [`GuestSession::protocol`]
/// before calling: a version 1 service never receives a principal, a
/// partition or `describe`, and a version 2 service answers `describe`
/// once per [`GuestSession::generation`] before its first call.
pub trait GuestSession: Send + Sync {
    fn invoke(&self, service: &str, operation: Operation) -> Result<Reply, Failure>;
    /// The application protocol `service` declares in this generation
    /// (AP §C.1). Version 1 by default.
    fn protocol(&self, _service: &str) -> u16 {
        merezha_domain::PROTOCOL_V1
    }
    /// The pinned catalog generation, which keys the `describe` check.
    fn generation(&self) -> u64 {
        0
    }
    fn invoke_until(
        &self,
        service: &str,
        operation: Operation,
        deadline: std::time::Instant,
    ) -> Result<Reply, Failure> {
        if std::time::Instant::now() >= deadline {
            return Err(Failure::transient("timeout"));
        }
        let result = self.invoke(service, operation);
        if std::time::Instant::now() >= deadline {
            return Err(Failure::transient("timeout"));
        }
        result
    }
}
pub trait Guest: Send + Sync {
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure>;
    fn services(&self) -> Result<Vec<String>, Failure>;
    /// How many guest calls may run at once, when the guest bounds them.
    /// PostgreSQL applications check it against HTTP admission plus
    /// background runs at startup (`invalid_concurrency_budget`).
    fn max_concurrent_invocations(&self) -> Option<usize> {
        None
    }
    /// Require at least `minimum` concurrent guest calls from now on,
    /// including after a reload: `invalid_concurrency_budget` when the
    /// guest admits fewer. A guest that can be reloaded refuses later
    /// generations below it.
    fn reserve_concurrency(&self, minimum: usize) -> Result<(), Failure> {
        match self.max_concurrent_invocations() {
            Some(available) if available < minimum => {
                Err(Failure::permanent("invalid_concurrency_budget"))
            }
            _ => Ok(()),
        }
    }
}
/// Policy is composed by the embedding host. It is called after typed payload
/// validation and before every command replay or query. Metadata is untrusted
/// unless the transport establishes an identity independently.
pub trait Authorizer: Send + Sync {
    fn authorize(
        &self,
        kind: MessageKind,
        tag: &str,
        stream: Option<&str>,
        payload: &Value,
        metadata: &Metadata,
    ) -> Result<(), Failure>;
}
/// Explicit policy for trusted local demos, never an authentication mechanism.
pub struct AllowAll;
impl Authorizer for AllowAll {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Ok(())
    }
}

impl Guest for merezha_runtime::Runtime {
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(
            merezha_runtime::Runtime::session(self).map_err(runtime_failure)?,
        ))
    }
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(self.catalog().map_err(runtime_failure)?.services)
    }
    fn max_concurrent_invocations(&self) -> Option<usize> {
        merezha_runtime::Runtime::limits(self)
            .ok()
            .map(|limits| limits.max_concurrent_invocations)
    }
    /// Also refuses later reloads below `minimum`.
    fn reserve_concurrency(&self, minimum: usize) -> Result<(), Failure> {
        self.require_concurrency(minimum)
            .map_err(|_| Failure::permanent("invalid_concurrency_budget"))
    }
}
impl GuestSession for merezha_runtime::Session {
    /// Sends the request in the service's declared protocol version, the
    /// operation fitted to it, and expects the response to echo it.
    fn invoke_until(
        &self,
        service: &str,
        operation: Operation,
        deadline: std::time::Instant,
    ) -> Result<Reply, Failure> {
        let kind = guest_operation(&operation);
        let version = merezha_runtime::Session::protocol(self, service);
        let operation = protocol::fit(operation, version)?;
        let input =
            merezha_domain::encode(&merezha_domain::Request::with_version(version, operation))?;
        let output = self
            .invoke_operation_until(service, kind, &input, deadline)
            .map_err(runtime_failure)?;
        reply(&output.output, version)
    }
    fn protocol(&self, service: &str) -> u16 {
        merezha_runtime::Session::protocol(self, service)
    }
    fn generation(&self) -> u64 {
        merezha_runtime::Session::generation(self)
    }
    fn invoke(&self, service: &str, operation: Operation) -> Result<Reply, Failure> {
        // The generation's own timeout bounds the call.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(24 * 3600);
        self.invoke_until(service, operation, deadline)
    }
}
/// The guest's protocol response, which must echo the request's `version`
/// (`protocol_version` otherwise). Output that is not a response (not JSON,
/// another shape or an invalid failure code) is `invalid_response`: the
/// guest broke the protocol, which counts as `failed` (TE §1.3), unlike a
/// payload the guest refused itself.
fn reply(output: &str, version: u16) -> Result<Reply, Failure> {
    let invalid = || Failure::permanent("invalid_response");
    let response: merezha_domain::Response =
        merezha_domain::decode(output).map_err(|_| invalid())?;
    if response.version != version {
        return Err(Failure::permanent("protocol_version"));
    }
    match response.outcome {
        merezha_domain::Outcome::Ok { reply } => Ok(reply),
        merezha_domain::Outcome::Err { error } if merezha_domain::valid_name(&error.code) => {
            Err(error)
        }
        merezha_domain::Outcome::Err { .. } => Err(invalid()),
    }
}
/// The span and metric name of a guest operation (TE §1.2).
fn guest_operation(operation: &Operation) -> merezha_runtime::GuestOperation {
    use merezha_runtime::GuestOperation;
    match operation {
        Operation::Validate { .. } => GuestOperation::Validate,
        Operation::Decide { .. } => GuestOperation::Decide,
        Operation::Project { .. } => GuestOperation::Project,
        Operation::Query { .. } => GuestOperation::Query,
        Operation::Describe {} => GuestOperation::Describe,
    }
}
fn runtime_failure(error: merezha_runtime::Error) -> Failure {
    use merezha_runtime::Error;
    match error {
        Error::Overloaded | Error::Timeout | Error::Internal => Failure::transient(error.code()),
        _ => Failure::permanent(error.code()),
    }
}
