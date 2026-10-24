//! The Merezha platform kernel: project configuration, the [`Module`]
//! contract, the shared [`AppContext`], cross-module [`ports`], the HTTP
//! [`edge`], health endpoints and the host lifecycle. Logs go through the
//! `merezha-telemetry` catalog; the `merezha` binary installs the
//! subscriber.
pub mod config;
pub mod context;
pub mod cron;
pub mod edge;
pub mod errors;
pub mod health;
pub mod host;
pub mod module;
pub mod net;
pub mod ports;

pub use config::{ConfigErrors, ConfigInput, ConfigIssue, ModuleSection, ProjectConfig};
pub use context::{AppContext, Clock, OsRandom, OutboundHttp, Random, SystemClock};
pub use edge::{ErrorReply, error_response};
pub use errors::{ErrorCode, ErrorRegistry, FieldError};
pub use host::{Host, HostParts, StartupError, Timeouts};
pub use merezha_postgres::{BoxFuture, Database, Migration, MigrationKind, Secret, Tx};
pub use module::{JobsPorts, JobsRuntime, Module, ModuleEvents};
pub use ports::{Principal, ResourceScope, Source};
