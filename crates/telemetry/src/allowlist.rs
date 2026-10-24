//! The export-time wall (TE §1.5 wall 2): the allowlist applied to every
//! span before it leaves the process, and the value caps shared with the
//! log paths.
use crate::{
    config::PrivacyConfig,
    names::{self, Gate},
    status::SignalAccount,
};
use opentelemetry::{Array, Context, KeyValue, StringValue, Value};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{Span, SpanData, SpanProcessor},
};
use std::{sync::Arc, time::Duration};

/// Exported span string values are cut to this many bytes, suffix `…`.
pub const SPAN_STRING_CAP: usize = 256;
/// Log string values (stderr and OTLP) are cut to this many bytes.
pub const LOG_STRING_CAP: usize = 512;
/// Attribute keys `tracing-opentelemetry` adds to every span event; they
/// are removed without counting as drops.
const BRIDGE_EVENT_KEYS: [&str; 2] = ["level", "target"];

/// Where an attribute is going.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Column {
    Span,
    Link,
    Metric,
    Log,
}

/// The allowlist table with the privacy switches applied.
#[derive(Clone, Copy, Debug)]
pub struct Allowlist {
    privacy: PrivacyConfig,
}
impl Allowlist {
    pub fn new(privacy: PrivacyConfig) -> Self {
        Self { privacy }
    }
    /// Whether `key` may appear in `column`.
    pub fn allows(&self, key: &str, column: Column) -> bool {
        let Some(row) = names::attribute(key) else {
            return false;
        };
        let gate_open = match row.gate {
            Gate::Always => true,
            Gate::TenantId => self.privacy.record_tenant_id,
            Gate::SubjectId => self.privacy.record_subject_id,
            Gate::ClientAddress => self.privacy.record_client_address,
        };
        gate_open
            && match column {
                Column::Span => row.span && !row.links_only,
                Column::Link => row.links_only || row.span,
                Column::Metric => row.metric,
                Column::Log => row.log,
            }
    }
}

/// Cut `text` to at most `cap` bytes on a character boundary, ending in
/// `…` when cut.
pub fn truncate(text: &str, cap: usize) -> Option<String> {
    if text.len() <= cap {
        return None;
    }
    let mut end = cap.saturating_sub('…'.len_utf8());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    Some(format!("{}…", &text[..end]))
}

fn cap_string(value: &StringValue, cap: usize) -> Option<StringValue> {
    truncate(value.as_str(), cap).map(StringValue::from)
}

/// `value` with every string cut to `cap` bytes.
pub fn cap_value(value: Value, cap: usize) -> Value {
    match value {
        Value::String(s) => match cap_string(&s, cap) {
            Some(cut) => Value::String(cut),
            None => Value::String(s),
        },
        Value::Array(Array::String(items)) => Value::Array(Array::String(
            items
                .into_iter()
                .map(|s| cap_string(&s, cap).unwrap_or(s))
                .collect(),
        )),
        other => other,
    }
}

/// Keep allowlisted attributes, capping strings; returns how many were
/// dropped.
fn filter(
    attributes: &mut Vec<KeyValue>,
    allowlist: &Allowlist,
    column: Column,
    silent: &[&str],
) -> u64 {
    let mut dropped = 0;
    attributes.retain(|kv| {
        let key = kv.key.as_str();
        if allowlist.allows(key, column) {
            true
        } else {
            if !silent.contains(&key) {
                dropped += 1;
            }
            false
        }
    });
    for kv in attributes.iter_mut() {
        if let Value::String(_) | Value::Array(Array::String(_)) = &kv.value {
            let value = std::mem::replace(&mut kv.value, Value::Bool(false));
            kv.value = cap_value(value, SPAN_STRING_CAP);
        }
    }
    dropped
}

/// Wraps the processor that queues spans: on end it owns the `SpanData`,
/// drops every key missing from the allowlist (counted in
/// `merezha.telemetry.attributes.dropped`) from the span, its events and
/// its links, and cuts strings to [`SPAN_STRING_CAP`] bytes. `Debug` names
/// the signal only, never the wrapped processor.
pub struct AllowlistSpanProcessor<P> {
    inner: P,
    allowlist: Allowlist,
    account: Arc<SignalAccount>,
}
impl<P> std::fmt::Debug for AllowlistSpanProcessor<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllowlistSpanProcessor")
            .field("signal", &self.account.signal())
            .finish_non_exhaustive()
    }
}
impl<P> AllowlistSpanProcessor<P> {
    pub fn new(inner: P, allowlist: Allowlist, account: Arc<SignalAccount>) -> Self {
        Self {
            inner,
            allowlist,
            account,
        }
    }
}
impl<P: SpanProcessor> SpanProcessor for AllowlistSpanProcessor<P> {
    fn on_start(&self, span: &mut Span, cx: &Context) {
        self.inner.on_start(span, cx);
    }
    fn on_end(&self, mut span: SpanData) {
        let list = &self.allowlist;
        let mut dropped = filter(&mut span.attributes, list, Column::Span, &[]);
        for event in &mut span.events.events {
            dropped += filter(
                &mut event.attributes,
                list,
                Column::Span,
                &BRIDGE_EVENT_KEYS,
            );
            if !names::is_event_name(&event.name) {
                // Event names come from the catalog; anything else could be
                // a formatted message.
                event.name = "dependency.log".into();
            }
        }
        for link in &mut span.links.links {
            dropped += filter(&mut link.attributes, list, Column::Link, &[]);
        }
        self.account.attributes_dropped(dropped);
        self.inner.on_end(span);
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn privacy_switches_gate_personal_keys() {
        let default = Allowlist::new(PrivacyConfig::default());
        assert!(default.allows("merezha.tenant.id", Column::Span));
        assert!(!default.allows("user.id", Column::Span));
        assert!(!default.allows("client.address", Column::Log));
        let open = Allowlist::new(PrivacyConfig {
            record_tenant_id: false,
            record_subject_id: true,
            record_client_address: true,
        });
        assert!(!open.allows("merezha.tenant.id", Column::Span));
        assert!(open.allows("user.id", Column::Span));
        assert!(!open.allows("user.id", Column::Metric));
        assert!(default.allows("merezha.link.reason", Column::Link));
        assert!(!default.allows("merezha.link.reason", Column::Span));
        assert!(!default.allows("http.route", Column::Log));
    }

    #[test]
    fn strings_are_cut_on_character_boundaries() {
        assert_eq!(truncate("short", 256), None);
        let cut = truncate(&"é".repeat(200), 256).unwrap();
        assert!(cut.len() <= 256 && cut.ends_with('…'), "{}", cut.len());
        let value = cap_value(Value::from("x".repeat(300)), 256);
        assert_eq!(value.as_str().len(), 256);
    }
}
