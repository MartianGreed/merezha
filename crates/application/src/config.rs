use merezha_domain::{Failure, Identity};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeSet;

/// Hard ceilings apply even when configuring a replaceable adapter.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub timeout_ms: u64,
    pub max_events: usize,
    pub max_streams: usize,
    pub max_store_bytes: usize,
    pub max_idempotency: usize,
    pub max_payload_bytes: usize,
    pub max_state_bytes: usize,
    pub batch_size: usize,
    pub max_concurrent: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            timeout_ms: 5000,
            max_events: 4096,
            max_streams: 256,
            max_store_bytes: 8_388_608,
            max_idempotency: 1024,
            max_payload_bytes: 16_384,
            max_state_bytes: 65_536,
            batch_size: 16,
            max_concurrent: 16,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<(), Failure> {
        if !(1..=30_000).contains(&self.timeout_ms) {
            return Err(Failure::permanent("invalid_limits"));
        }
        for (value, max) in [
            (self.max_events, 4096),
            (self.max_streams, 256),
            (self.max_store_bytes, 8_388_608),
            (self.max_idempotency, 1024),
            (self.max_payload_bytes, 65_536),
            (self.max_state_bytes, 262_144),
            (self.batch_size, 64),
            (self.max_concurrent, 64),
        ] {
            if value == 0 || value > max {
                return Err(Failure::permanent("invalid_limits"));
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandRoute {
    pub tag: Identity,
    pub service: Identity,
    pub aggregate: Identity,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRoute {
    pub tag: Identity,
    pub service: Identity,
    pub projection: Identity,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    pub name: Identity,
    pub service: Identity,
    pub initial: Value,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u16,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub commands: Vec<CommandRoute>,
    #[serde(default)]
    pub queries: Vec<QueryRoute>,
    #[serde(default)]
    pub projections: Vec<Projection>,
}
impl Config {
    pub fn parse(input: &str) -> Result<Self, Failure> {
        if input.len() > 65_536 {
            return Err(Failure::permanent("config_limit"));
        }
        let config: Self =
            toml::from_str(input).map_err(|_| Failure::permanent("invalid_config"))?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), Failure> {
        self.limits.validate()?;
        if self.version != 1
            || self.commands.len() > 64
            || self.queries.len() > 64
            || self.projections.len() > 32
        {
            return Err(Failure::permanent("invalid_config"));
        }
        let commands: BTreeSet<_> = self.commands.iter().map(|r| &r.tag).collect();
        let queries: BTreeSet<_> = self.queries.iter().map(|r| &r.tag).collect();
        let projections: BTreeSet<_> = self.projections.iter().map(|r| &r.name).collect();
        if commands.len() != self.commands.len()
            || queries.len() != self.queries.len()
            || projections.len() != self.projections.len()
            || !commands.is_disjoint(&queries)
            || self
                .queries
                .iter()
                .any(|q| !projections.contains(&q.projection))
            || self.projections.iter().any(|p| {
                serde_json::to_vec(&p.initial)
                    .map_or(true, |v| v.len() > self.limits.max_state_bytes)
            })
        {
            return Err(Failure::permanent("invalid_routes"));
        }
        Ok(())
    }
}
