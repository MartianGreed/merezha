//! The inbox stream's event-stream parser (NT-N5) and a reader over a
//! response body.
use axum::body::{Body, HttpBody};
use std::{collections::VecDeque, pin::Pin, time::Duration};

/// One dispatched block of an event stream: the lines up to a blank line.
/// Comment-only and `retry`-only blocks are kept, so a test sees
/// heartbeats and the reconnection delay.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SseEvent {
    /// The `event:` field, `None` when absent (a plain `message`).
    pub event: Option<String>,
    /// The `id:` field of this block, `None` when absent.
    pub id: Option<String>,
    /// The `data:` lines joined with `\n`.
    pub data: String,
    /// The `retry:` field, when it is a decimal number of milliseconds.
    pub retry: Option<u64>,
    /// Comment lines (`: text`), without the colon and one leading space.
    pub comments: Vec<String>,
}
impl SseEvent {
    /// The event's `data` as JSON.
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.data).unwrap_or(serde_json::Value::Null)
    }
    /// Whether the block is a heartbeat comment and nothing else.
    pub fn is_comment(&self) -> bool {
        self.event.is_none()
            && self.id.is_none()
            && self.data.is_empty()
            && self.retry.is_none()
            && !self.comments.is_empty()
    }
}

/// The `text/event-stream` grammar of the HTML Living Standard: lines end
/// in LF, CRLF or CR; a blank line dispatches; `field: value` drops one
/// space after the colon; a line starting with `:` is a comment; a UTF-8
/// BOM at the start is skipped. Feed bytes as they arrive.
#[derive(Debug, Default)]
pub struct EventStreamParser {
    buffer: Vec<u8>,
    current: SseEvent,
    data_lines: Vec<String>,
    started: bool,
    /// A CR ended the last line: a following LF belongs to it.
    after_cr: bool,
    touched: bool,
}
impl EventStreamParser {
    pub fn new() -> Self {
        Self::default()
    }
    /// Feed `bytes`; returns every block they complete.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        let mut out = Vec::new();
        for &byte in bytes {
            if self.after_cr {
                self.after_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\n' => self.line(&mut out),
                b'\r' => {
                    self.line(&mut out);
                    self.after_cr = true;
                }
                _ => self.buffer.push(byte),
            }
        }
        out
    }
    fn line(&mut self, out: &mut Vec<SseEvent>) {
        let mut raw = std::mem::take(&mut self.buffer);
        if !self.started {
            self.started = true;
            if raw.starts_with(&[0xEF, 0xBB, 0xBF]) {
                raw.drain(..3);
            }
        }
        let line = String::from_utf8_lossy(&raw).into_owned();
        if line.is_empty() {
            if self.touched {
                let mut event = std::mem::take(&mut self.current);
                event.data = std::mem::take(&mut self.data_lines).join("\n");
                out.push(event);
            }
            self.touched = false;
            return;
        }
        self.touched = true;
        if let Some(comment) = line.strip_prefix(':') {
            let comment = comment.strip_prefix(' ').unwrap_or(comment);
            self.current.comments.push(comment.to_owned());
            return;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line.as_str(), ""),
        };
        match field {
            "event" => self.current.event = Some(value.to_owned()),
            "data" => self.data_lines.push(value.to_owned()),
            "id" if !value.contains('\0') => self.current.id = Some(value.to_owned()),
            "retry" if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => {
                self.current.retry = value.parse().ok();
            }
            _ => {}
        }
    }
}

/// Reads the events of a streaming response body.
pub struct EventReader {
    body: Body,
    parser: EventStreamParser,
    pending: VecDeque<SseEvent>,
    ended: bool,
}
impl std::fmt::Debug for EventReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventReader")
            .field("pending", &self.pending.len())
            .field("ended", &self.ended)
            .finish()
    }
}
impl EventReader {
    pub fn new(body: Body) -> Self {
        Self {
            body,
            parser: EventStreamParser::new(),
            pending: VecDeque::new(),
            ended: false,
        }
    }
    /// The next block within `within`: `Ok(None)` once the body ended,
    /// `Err(())` on a timeout.
    #[allow(clippy::result_unit_err)]
    pub async fn next(&mut self, within: Duration) -> Result<Option<SseEvent>, ()> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            if self.ended {
                return Ok(None);
            }
            let frame = std::future::poll_fn(|cx| Pin::new(&mut self.body).poll_frame(cx));
            match tokio::time::timeout_at(deadline, frame).await {
                Err(_) => return Err(()),
                Ok(None) | Ok(Some(Err(_))) => self.ended = true,
                Ok(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data() {
                        self.pending.extend(self.parser.feed(&data));
                    }
                }
            }
        }
    }
    /// The next block that is not a heartbeat comment, within `within`.
    #[allow(clippy::result_unit_err)]
    pub async fn next_event(&mut self, within: Duration) -> Result<Option<SseEvent>, ()> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next(left).await? {
                Some(event) if event.is_comment() => continue,
                other => return Ok(other),
            }
        }
    }
    /// Whether the body ended within `within`, skipping what came first.
    pub async fn ends_within(&mut self, within: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next(left).await {
                Ok(None) => return true,
                Ok(Some(_)) => {}
                Err(()) => return false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_parser_follows_the_event_stream_grammar() {
        let mut parser = EventStreamParser::new();
        let mut events =
            parser.feed(b"\xEF\xBB\xBFretry: 5000\n\nevent: item\nid: 7\ndata: {\"a\":");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].retry, Some(5000));
        events = parser.feed(b"1}\n\n: hb\n\nevent:hello\r\nid:3\r\ndata:x\r\ndata: y\r\r");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event.as_deref(), Some("item"));
        assert_eq!(events[0].id.as_deref(), Some("7"));
        assert_eq!(events[0].json(), serde_json::json!({"a": 1}));
        assert!(events[1].is_comment());
        assert_eq!(events[1].comments, ["hb"]);
        assert_eq!(events[2].event.as_deref(), Some("hello"));
        assert_eq!(events[2].id.as_deref(), Some("3"));
        assert_eq!(events[2].data, "x\ny");
        // Unknown fields and a bad retry are ignored; blank lines alone
        // dispatch nothing.
        events = parser.feed(b"\n\nfoo: bar\nretry: 1x\ndata\n\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].retry, None);
        assert_eq!(events[0].data, "");
    }

    #[test]
    fn a_split_crlf_is_one_line_end() {
        let mut parser = EventStreamParser::new();
        assert!(parser.feed(b"data: a\r").is_empty());
        let events = parser.feed(b"\n\r\n");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "a");
    }
}
