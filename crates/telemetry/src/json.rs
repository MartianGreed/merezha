//! The flat JSON event formatter for stderr (TE §4 layer 1). It replaces
//! `merezha_cli::emit` and keeps its field names:
//!
//! ```json
//! {"timestamp_unix_ms":1761220000000,"level":"info","event":"listener.ready",
//!  "service":"tasks","version":"0.1.0","instance":"…","trace_id":"…",
//!  "span_id":"…","request_id":"…","address":"127.0.0.1:8080"}
//! ```
//!
//! - `event` is the `tracing` metadata name set by [`crate::event!`], or
//!   the legacy `event` field of `tracing::info!(event = "…", …)` calls.
//! - Catalog events keep only the fields their [`crate::names::EVENTS`] row
//!   lists; strings are cut to 512 bytes. Uncatalogued `merezha*` events
//!   keep their fields, capped, and fail the catalog test instead. A field
//!   named like an envelope key (`version` on migration events) is written
//!   as `<area>_<field>` (`migration_version`); a catalog `trace_id` or
//!   `span_id` field fills the envelope key when no recording span did.
//! - `trace_id` and `span_id` appear inside a recording span; the scope
//!   fields `request_id`, `correlation_id`, `causation_id` and `message_id`
//!   are flattened in from the enclosing spans.
//! - Events from crates outside `merezha*` keep only `target`, `level` and
//!   the fixed name `dependency.log`; their message is dropped. The stderr
//!   filter refuses the exporter's own crates altogether
//!   (`subscriber::EXPORTER_TARGETS`).
use crate::{
    allowlist::{LOG_STRING_CAP, truncate},
    config::Service,
    names,
};
use serde_json::{Map, Value};
use std::{
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span,
};
use tracing_subscriber::{
    fmt::MakeWriter,
    layer::{Context, Layer},
    registry::LookupSpan,
};

/// The fixed name of a third-party event.
pub const DEPENDENCY_EVENT: &str = "dependency.log";
/// Scope fields flattened into every line, by short name. Span fields may
/// use the short name or the `merezha.` attribute key.
pub const SCOPE_FIELDS: [&str; 4] = ["request_id", "correlation_id", "causation_id", "message_id"];
const RESERVED: [&str; 10] = [
    "timestamp_unix_ms",
    "level",
    "event",
    "service",
    "version",
    "instance",
    "trace_id",
    "span_id",
    "target",
    "message",
];

/// A recorded field value.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Str(String),
    U64(u64),
    I64(i64),
    F64(f64),
    Bool(bool),
}
impl FieldValue {
    fn to_json(&self, cap: usize) -> Value {
        match self {
            Self::Str(s) => Value::String(truncate(s, cap).unwrap_or_else(|| s.clone())),
            Self::U64(v) => Value::from(*v),
            Self::I64(v) => Value::from(*v),
            Self::F64(v) => serde_json::Number::from_f64(*v).map_or(Value::Null, Value::Number),
            Self::Bool(v) => Value::Bool(*v),
        }
    }
}

/// Every field of an event except error values, which are never recorded
/// (their `Display` may quote input).
#[derive(Default, Debug)]
pub struct Fields {
    pub values: Vec<(&'static str, FieldValue)>,
    pub message: Option<String>,
}
impl Fields {
    pub fn of(event: &Event<'_>) -> Self {
        let mut fields = Self::default();
        event.record(&mut fields);
        fields
    }
    fn get(&self, name: &str) -> Option<&FieldValue> {
        self.values.iter().find(|(n, _)| *n == name).map(|(_, v)| v)
    }
}
impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_owned());
        } else {
            self.values
                .push((field.name(), FieldValue::Str(value.to_owned())));
        }
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.values.push((field.name(), FieldValue::U64(value)));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.values.push((field.name(), FieldValue::I64(value)));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.values.push((field.name(), FieldValue::F64(value)));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.values.push((field.name(), FieldValue::Bool(value)));
    }
    fn record_error(&mut self, _field: &Field, _value: &(dyn std::error::Error + 'static)) {}
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{value:?}"));
        } else {
            self.values
                .push((field.name(), FieldValue::Str(format!("{value:?}"))));
        }
    }
}

/// The catalog name of an event: its metadata name when catalog-shaped,
/// else a legacy `event` string field.
pub fn event_name<'a>(event: &Event<'_>, fields: &'a Fields) -> Option<std::borrow::Cow<'a, str>> {
    let name = event.metadata().name();
    if names::is_event_name(name) {
        return Some(std::borrow::Cow::Borrowed(name));
    }
    match fields.get("event") {
        Some(FieldValue::Str(legacy)) => Some(std::borrow::Cow::Borrowed(legacy.as_str())),
        _ => None,
    }
}

/// Whether `target` belongs to a Merezha crate.
pub fn is_merezha(target: &str) -> bool {
    target.starts_with("merezha")
}

/// Scope fields recorded on a span.
#[derive(Default, Debug, Clone)]
struct Scope(Vec<(&'static str, String)>);
impl Visit for Scope {
    fn record_str(&mut self, field: &Field, value: &str) {
        let name = field.name();
        let short = name.strip_prefix("merezha.").unwrap_or(name);
        if let Some(known) = SCOPE_FIELDS.iter().find(|s| **s == short) {
            self.0.retain(|(n, _)| n != known);
            self.0.push((known, value.to_owned()));
        }
    }
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

/// The `tracing` layer writing one JSON line per event.
pub struct JsonLayer<W> {
    writer: W,
    service: String,
    instance: Option<String>,
}
impl<W> JsonLayer<W> {
    pub fn new(writer: W, service: &Service) -> Self {
        Self {
            writer,
            service: service.name.clone(),
            instance: service.instance_id.clone(),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

fn trace_ids() -> Option<(String, String)> {
    use opentelemetry::trace::TraceContextExt;
    let context = opentelemetry::Context::current();
    let span = context.span();
    let span_context = span.span_context();
    (span_context.is_valid() && span_context.is_sampled()).then(|| {
        (
            span_context.trace_id().to_string(),
            span_context.span_id().to_string(),
        )
    })
}

impl<W> JsonLayer<W> {
    /// The JSON object for `event`.
    fn render<S>(&self, event: &Event<'_>, ctx: &Context<'_, S>) -> Map<String, Value>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let meta = event.metadata();
        let fields = Fields::of(event);
        let mut line = Map::new();
        line.insert("timestamp_unix_ms".into(), Value::from(now_ms()));
        line.insert(
            "level".into(),
            Value::from(meta.level().as_str().to_ascii_lowercase()),
        );
        line.insert("service".into(), Value::from(self.service.clone()));
        line.insert("version".into(), Value::from(Service::HOST_VERSION));
        if let Some(instance) = &self.instance {
            line.insert("instance".into(), Value::from(instance.clone()));
        }
        if let Some((trace_id, span_id)) = trace_ids() {
            line.insert("trace_id".into(), Value::from(trace_id));
            line.insert("span_id".into(), Value::from(span_id));
        }
        if !is_merezha(meta.target()) {
            line.insert("event".into(), Value::from(DEPENDENCY_EVENT));
            line.insert("target".into(), Value::from(meta.target()));
            return line;
        }
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                if let Some(fields) = span.extensions().get::<Scope>() {
                    for (name, value) in &fields.0 {
                        if !line.contains_key(*name) {
                            let cut =
                                truncate(value, LOG_STRING_CAP).unwrap_or_else(|| value.clone());
                            line.insert((*name).into(), Value::from(cut));
                        }
                    }
                }
            }
        }
        let name = event_name(event, &fields);
        let row = name.as_deref().and_then(names::event);
        line.insert(
            "event".into(),
            Value::from(name.as_deref().unwrap_or("unnamed").to_owned()),
        );
        let area = name
            .as_deref()
            .and_then(|n| n.split('.').next())
            .unwrap_or("event")
            .to_owned();
        for (field, value) in &fields.values {
            if *field == "event" {
                continue;
            }
            let json = match row {
                Some(row) => match row.field(field) {
                    None => continue,
                    Some(spec) if spec.json => match value {
                        FieldValue::Str(text) => serde_json::from_str(text)
                            .unwrap_or_else(|_| value.to_json(LOG_STRING_CAP)),
                        _ => value.to_json(LOG_STRING_CAP),
                    },
                    Some(_) => value.to_json(LOG_STRING_CAP),
                },
                None => value.to_json(LOG_STRING_CAP),
            };
            // A trace id the event carries itself (the kernel edge's, until
            // edge spans exist) fills the envelope key when no recording
            // span did; the span context wins otherwise.
            if matches!(*field, "trace_id" | "span_id") {
                if !line.contains_key(*field) {
                    line.insert((*field).to_owned(), json);
                }
                continue;
            }
            // Any other field named like an envelope key (`version` on
            // migration events) is written as `<area>_<field>`.
            let key = if RESERVED.contains(field) {
                format!("{area}_{field}")
            } else {
                (*field).to_owned()
            };
            line.insert(key, json);
        }
        line
    }
}

impl<S, W> Layer<S> for JsonLayer<W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'w> MakeWriter<'w> + 'static,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut scope = Scope::default();
        attrs.record(&mut scope);
        if !scope.0.is_empty()
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(scope);
        }
    }
    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            let mut scope = extensions.remove::<Scope>().unwrap_or_default();
            values.record(&mut scope);
            if !scope.0.is_empty() {
                extensions.insert(scope);
            }
        }
    }
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let line = self.render(event, &ctx);
        let mut bytes = Value::Object(line).to_string().into_bytes();
        bytes.push(b'\n');
        // One write per line, so lines never interleave; a lossy writer
        // drops whole lines.
        let _ = self
            .writer
            .make_writer_for(event.metadata())
            .write_all(&bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> MakeWriter<'a> for Buffer {
        type Writer = Buffer;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }
    fn lines(buffer: &Buffer) -> Vec<Value> {
        String::from_utf8(buffer.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
    fn capture(service: &Service, f: impl FnOnce()) -> Vec<Value> {
        let buffer = Buffer::default();
        let subscriber =
            tracing_subscriber::registry().with(JsonLayer::new(buffer.clone(), service));
        tracing::subscriber::with_default(subscriber, f);
        lines(&buffer)
    }

    #[test]
    fn lines_keep_the_emit_field_names() {
        let lines = capture(&Service::raw(), || {
            crate::event!(
                INFO,
                "listener.ready",
                address = "127.0.0.1:1",
                generation = 1u64
            );
        });
        let line = &lines[0];
        assert_eq!(line["event"], "listener.ready");
        assert_eq!(line["level"], "info");
        assert_eq!(line["service"], "merezha");
        assert_eq!(line["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(line["address"], "127.0.0.1:1");
        assert_eq!(line["generation"], 1);
        assert!(line["timestamp_unix_ms"].as_u64().unwrap() > 1_700_000_000_000);
        assert!(line.get("message").is_none() && line.get("instance").is_none());
    }

    #[test]
    fn catalog_events_keep_only_catalog_fields_and_legacy_events_keep_their_name() {
        let mut service = Service::raw();
        service.name = "tasks".into();
        service.instance_id = Some("tasks-a:1".into());
        let lines = capture(&service, || {
            crate::event!(
                ERROR,
                "startup.failed",
                code = "invalid_config",
                issues = r#"[{"key":"app.id","code":"missing_key","origin":"file"}]"#,
                secret = "s3cr3t"
            );
            tracing::warn!(event = "migration.pending", module = "app", version = 3u64);
        });
        assert_eq!(lines[0]["level"], "error");
        assert_eq!(lines[0]["issues"][0]["key"], "app.id");
        assert!(lines[0].get("secret").is_none(), "{}", lines[0]);
        assert_eq!(lines[0]["instance"], "tasks-a:1");
        assert_eq!(lines[1]["event"], "migration.pending");
        assert_eq!(
            lines[1]["version"],
            env!("CARGO_PKG_VERSION"),
            "envelope keys win"
        );
        assert_eq!(lines[1]["migration_version"], 3);
    }

    #[test]
    fn an_event_trace_id_fills_the_envelope_key() {
        let lines = capture(&Service::raw(), || {
            crate::event!(
                INFO,
                "http.request.completed",
                method = "GET",
                status = 200u64,
                trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
            );
        });
        assert_eq!(lines[0]["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert!(lines[0].get("http_trace_id").is_none(), "{}", lines[0]);
    }

    #[test]
    fn scope_fields_flatten_and_values_are_capped() {
        let lines = capture(&Service::raw(), || {
            let span = tracing::info_span!("request", request_id = "r-1", other = "x");
            let _entered = span.enter();
            let inner = tracing::info_span!("command", "merezha.correlation_id" = "c-1");
            let _inner = inner.enter();
            tracing::info!(
                event = "uncatalogued.thing",
                note = "y".repeat(600).as_str()
            );
        });
        assert_eq!(lines[0]["request_id"], "r-1");
        assert_eq!(lines[0]["correlation_id"], "c-1");
        assert!(lines[0].get("other").is_none());
        assert!(lines[0]["note"].as_str().unwrap().len() <= LOG_STRING_CAP);
    }
}
