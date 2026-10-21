//! Bounded, process-local application orchestration. Host implementation follows
//! the versioned guest contract in `merezha-domain`.
pub mod config;

use merezha_domain::{Failure, MessageKind, Metadata, Operation, Reply};
use serde_json::Value;

/// A pinned guest generation for a complete dispatch. Implementations must
/// bound execution, input and output and create a fresh guest per call.
pub trait GuestSession: Send + Sync {
    fn invoke(&self, service: &str, operation: Operation) -> Result<Reply, Failure>;
}
pub trait Guest: Send + Sync {
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure>;
    fn services(&self) -> Result<Vec<String>, Failure>;
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
