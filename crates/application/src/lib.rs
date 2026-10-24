//! Bounded application orchestration with memory, owned SQLite or (feature
//! `postgres`) shared PostgreSQL storage. The host follows the versioned
//! guest contract in `merezha-domain`.
mod application;
pub mod config;
mod durable;
#[cfg(feature = "postgres")]
pub mod postgres;
mod sqlite;
pub mod status;
pub mod store;
pub use application::{Application, ProjectionState, QueryResult};

use merezha_domain::{Failure, MessageKind, Metadata, Operation, Reply};
use serde_json::Value;

/// A pinned guest generation for a complete dispatch. Implementations must
/// bound execution, input and output and create a fresh guest per call.
pub trait GuestSession: Send + Sync {
    fn invoke(&self, service: &str, operation: Operation) -> Result<Reply, Failure>;
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
}
impl GuestSession for merezha_runtime::Session {
    fn invoke_until(
        &self,
        service: &str,
        operation: Operation,
        deadline: std::time::Instant,
    ) -> Result<Reply, Failure> {
        let input = merezha_domain::encode(&merezha_domain::Request::new(operation))?;
        let output = self
            .invoke_service_until(service, &input, deadline)
            .map_err(runtime_failure)?;
        merezha_domain::Response::decode(&output.output)
    }
    fn invoke(&self, service: &str, operation: Operation) -> Result<Reply, Failure> {
        let input = merezha_domain::encode(&merezha_domain::Request::new(operation))?;
        let output = self
            .invoke_service(service, &input)
            .map_err(runtime_failure)?;
        merezha_domain::Response::decode(&output.output)
    }
}
fn runtime_failure(error: merezha_runtime::Error) -> Failure {
    use merezha_runtime::Error;
    match error {
        Error::Overloaded | Error::Timeout | Error::Internal => Failure::transient(error.code()),
        _ => Failure::permanent(error.code()),
    }
}
