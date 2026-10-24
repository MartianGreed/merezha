//! Stored trace context (TE §2.2, §2.3; R10): the `trace_parent` and
//! `trace_state` columns of `merezha_app.events`, `merezha_jobs.outbox` and
//! `merezha_jobs.jobs`. This module owns their format; AP's migrations own
//! the DDL. W3C `traceparent`, version `00`, exactly 55 bytes; `tracestate`
//! at most 512 bytes and 32 members.
//!
//! The edge trust policy (T2), event and job filling (T3, T5) and span
//! links build on these helpers.
use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
use std::str::FromStr;

/// Length of a version-00 `traceparent`.
pub const TRACE_PARENT_LEN: usize = 55;
/// Largest stored `tracestate`.
pub const MAX_TRACE_STATE: usize = 512;
/// Most `tracestate` members.
pub const MAX_TRACE_STATE_MEMBERS: usize = 32;

/// A span context as stored in a row: a valid `traceparent`, sampled or
/// not, and an optional `tracestate`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredContext {
    trace_parent: String,
    trace_state: Option<String>,
}
impl StoredContext {
    /// Validate stored columns. Malformed values yield `None`, so a bad row
    /// starts a new trace instead of failing the work.
    pub fn parse(trace_parent: &str, trace_state: Option<&str>) -> Option<Self> {
        let context = parse_traceparent(trace_parent, trace_state)?;
        Some(Self::from_span_context(&context))
    }
    /// The stored form of a span context.
    pub fn from_span_context(context: &SpanContext) -> Self {
        let state = context.trace_state().header();
        Self {
            trace_parent: format_traceparent(context),
            trace_state: (!state.is_empty()).then_some(state),
        }
    }
    /// The context of the current `tracing` span, when it has a valid one.
    pub fn current() -> Option<Self> {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let context = tracing::Span::current().context();
        let span = context.span();
        let span_context = span.span_context();
        span_context
            .is_valid()
            .then(|| Self::from_span_context(span_context))
    }
    pub fn trace_parent(&self) -> &str {
        &self.trace_parent
    }
    pub fn trace_state(&self) -> Option<&str> {
        self.trace_state.as_deref()
    }
    /// The remote span context to parent or link to.
    pub fn span_context(&self) -> SpanContext {
        parse_traceparent(&self.trace_parent, self.trace_state.as_deref())
            .unwrap_or_else(SpanContext::empty_context)
    }
}

fn hex_id<const N: usize>(text: &str) -> Option<[u8; N]> {
    if text.len() != N * 2
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).ok()?;
    }
    out.iter().any(|b| *b != 0).then_some(out)
}

/// Parse a version-00 `traceparent` plus optional `tracestate` into a
/// remote span context. Malformed input, all-zero ids, unknown versions and
/// oversized state yield `None`.
pub fn parse_traceparent(trace_parent: &str, trace_state: Option<&str>) -> Option<SpanContext> {
    if trace_parent.len() != TRACE_PARENT_LEN {
        return None;
    }
    let mut parts = trace_parent.split('-');
    let (version, trace, span, flags) =
        (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    if version != "00" || parts.next().is_some() {
        return None;
    }
    let trace = TraceId::from_bytes(hex_id::<16>(trace)?);
    let span = SpanId::from_bytes(hex_id::<8>(span)?);
    let flags = u8::from_str_radix(flags, 16).ok().filter(|_| {
        flags.len() == 2
            && flags
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })?;
    let state = match trace_state {
        None | Some("") => TraceState::default(),
        Some(text)
            if text.len() <= MAX_TRACE_STATE
                && text.split(',').count() <= MAX_TRACE_STATE_MEMBERS =>
        {
            TraceState::from_str(text).ok()?
        }
        Some(_) => return None,
    };
    Some(SpanContext::new(
        trace,
        span,
        TraceFlags::new(flags & 0x01),
        true,
        state,
    ))
}

/// Format a span context as a version-00 `traceparent`.
pub fn format_traceparent(context: &SpanContext) -> String {
    format!(
        "00-{}-{}-{:02x}",
        context.trace_id(),
        context.span_id(),
        context.trace_flags().to_u8() & 0x01
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn traceparent_round_trips_with_flags_and_state() {
        let stored = StoredContext::parse(PARENT, Some("vendor=abc")).unwrap();
        assert_eq!(stored.trace_parent(), PARENT);
        assert_eq!(stored.trace_parent().len(), TRACE_PARENT_LEN);
        assert_eq!(stored.trace_state(), Some("vendor=abc"));
        let context = stored.span_context();
        assert!(context.is_sampled() && context.is_remote());
        let unsampled = PARENT.replace("-01", "-00");
        let stored = StoredContext::parse(&unsampled, None).unwrap();
        assert!(!stored.span_context().is_sampled());
        assert_eq!(stored.trace_parent(), unsampled);
    }

    #[test]
    fn malformed_contexts_are_ignored() {
        for bad in [
            "",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-",
            "00_4bf92f3577b34da6a3ce929d0e0e4736_00f067aa0ba902b7_01",
        ] {
            assert!(StoredContext::parse(bad, None).is_none(), "{bad}");
        }
        let long_state = format!("a={}", "x".repeat(600));
        assert!(StoredContext::parse(PARENT, Some(&long_state)).is_none());
        let many = (0..33)
            .map(|i| format!("k{i}=v"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(StoredContext::parse(PARENT, Some(&many)).is_none());
    }

    #[test]
    fn no_current_context_outside_a_recording_span() {
        assert!(StoredContext::current().is_none());
    }
}
