//! The Merezha platform kernel: project configuration, the [`Module`]
//! contract, the shared [`AppContext`], cross-module [`ports`], the HTTP
//! [`edge`], health endpoints, structured logging and the host lifecycle.
pub mod config;
pub mod context;
pub mod edge;
pub mod health;
pub mod host;
pub mod module;
pub mod net;
pub mod ports;
pub mod telemetry;

pub use config::{ConfigErrors, ConfigInput, ConfigIssue, ModuleSection, ProjectConfig};
pub use context::{AppContext, Clock, OsRandom, OutboundHttp, Random, SystemClock};
pub use edge::error_response;
pub use host::{Host, HostParts, StartupError, Timeouts};
pub use merezha_postgres::{BoxFuture, Database, Migration, Secret};
pub use module::{Module, ModuleEvents};
pub use ports::Principal;
