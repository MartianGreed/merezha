//! Exact drop accounting (TE §3): through the real SDK batch processors,
//! `exported + failed + dropped + pending == emitted` per signal, refusals
//! once `pending >= max_queue`, failures counted per item, and whatever is
//! left at shutdown counted once as `dropped{reason=shutdown}`.
use merezha_telemetry::{
    names::TelemetrySignal,
    pipeline::{Counting, GuardedLogProcessor, GuardedSpanProcessor},
    status::SignalAccount,
};
use opentelemetry::{
    logs::{LogRecord as _, Logger as _, LoggerProvider as _},
    trace::{Span as _, Tracer as _, TracerProvider as _},
};
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    logs::{BatchLogProcessor, LogBatch, LogExporter, SdkLoggerProvider},
    trace::{BatchSpanProcessor, SdkTracerProvider, SpanData, SpanExporter},
};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// An exporter that blocks while closed and fails while `fail` is set.
#[derive(Clone, Debug, Default)]
struct Gate {
    closed: Arc<(Mutex<bool>, Condvar)>,
    fail: Arc<AtomicBool>,
}
impl Gate {
    fn set_closed(&self, closed: bool) {
        *self.closed.0.lock().unwrap() = closed;
        self.closed.1.notify_all();
    }
    fn pass(&self) -> OTelSdkResult {
        let mut closed = self.closed.0.lock().unwrap();
        while *closed {
            closed = self.closed.1.wait(closed).unwrap();
        }
        if self.fail.load(Ordering::SeqCst) {
            Err(OTelSdkError::InternalFailure("refused".into()))
        } else {
            Ok(())
        }
    }
}
impl SpanExporter for Gate {
    async fn export(&self, _batch: Vec<SpanData>) -> OTelSdkResult {
        self.pass()
    }
}
impl LogExporter for Gate {
    async fn export(&self, _batch: LogBatch<'_>) -> OTelSdkResult {
        self.pass()
    }
}

const MAX_QUEUE: u32 = 10;
const MAX_BATCH: u32 = 4;

fn tracer_provider(gate: &Gate, account: &Arc<SignalAccount>) -> SdkTracerProvider {
    let batch = BatchSpanProcessor::builder(Counting::new(gate.clone(), account.clone()))
        .with_batch_config(
            opentelemetry_sdk::trace::BatchConfigBuilder::default()
                .with_max_queue_size((MAX_QUEUE + MAX_BATCH) as usize)
                .with_max_export_batch_size(MAX_BATCH as usize)
                .with_scheduled_delay(Duration::from_millis(20))
                .build(),
        )
        .build();
    SdkTracerProvider::builder()
        .with_span_processor(GuardedSpanProcessor::new(batch, account.clone()))
        .build()
}

fn settle_until(
    account: &SignalAccount,
    done: impl Fn(&merezha_telemetry::status::SignalCounts) -> bool,
) {
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(20) {
        if done(&account.counts()) {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("accounting did not settle: {:?}", account.counts());
}

#[test]
fn spans_balance_through_refusal_failure_and_shutdown() {
    let gate = Gate::default();
    let account = Arc::new(SignalAccount::new(TelemetrySignal::Traces, MAX_QUEUE));
    let provider = tracer_provider(&gate, &account);
    let tracer = provider.tracer("accounting");

    // Collector stalled: at most max_queue spans are held (queued plus in
    // flight); every other span end is refused at once.
    gate.set_closed(true);
    for _ in 0..100 {
        tracer.start("startup").end();
    }
    let c = account.counts();
    assert_eq!((c.emitted, c.pending, c.dropped_queue_full), (100, 10, 90));
    assert!(c.balanced());

    gate.set_closed(false);
    settle_until(&account, |c| c.pending == 0);
    assert_eq!(account.counts().exported, 10);

    // Failing collector: every item of a failed batch counts as failed.
    gate.fail.store(true, Ordering::SeqCst);
    for _ in 0..5 {
        tracer.start("startup").end();
    }
    let _ = provider.force_flush();
    settle_until(&account, |c| c.pending == 0);
    assert_eq!(account.counts().failed, 5);
    assert_eq!(account.counts().last_code, "export_failed");
    gate.fail.store(false, Ordering::SeqCst);

    // Shutdown while stalled: the flush times out and what is left counts
    // once as dropped{shutdown}, even when the stalled batch finishes later.
    gate.set_closed(true);
    for _ in 0..3 {
        tracer.start("startup").end();
    }
    account.close();
    let _ = provider.shutdown_with_timeout(Duration::from_millis(100));
    account.finalize();
    // After shutdown the SDK hands no span to any processor.
    tracer.start("after").end();
    gate.set_closed(false);
    std::thread::sleep(Duration::from_millis(100));
    let c = account.counts();
    assert_eq!(
        (
            c.emitted,
            c.exported,
            c.failed,
            c.dropped_queue_full,
            c.dropped_shutdown,
            c.pending
        ),
        (108, 10, 5, 90, 3, 0)
    );
    assert!(c.balanced());
}

#[test]
fn concurrent_emitters_never_break_the_balance() {
    let gate = Gate::default();
    let account = Arc::new(SignalAccount::new(TelemetrySignal::Traces, MAX_QUEUE));
    let provider = tracer_provider(&gate, &account);
    std::thread::scope(|scope| {
        for t in 0..8 {
            let tracer = provider.tracer("accounting");
            let gate = gate.clone();
            scope.spawn(move || {
                for i in 0..5_000 {
                    if t == 0 && i % 500 == 0 {
                        gate.set_closed(i % 1_000 == 0);
                    }
                    tracer.start("startup").end();
                }
            });
        }
    });
    gate.set_closed(false);
    account.close();
    provider
        .shutdown_with_timeout(Duration::from_secs(5))
        .unwrap();
    account.finalize();
    let c = account.counts();
    assert_eq!(c.emitted, 40_000);
    assert!(c.balanced(), "{c:?}");
    assert_eq!(c.pending, 0);
    assert!(c.dropped_queue_full > 0 && c.exported > 0, "{c:?}");
}

#[test]
fn logs_balance_through_refusal_and_shutdown() {
    let gate = Gate::default();
    let account = Arc::new(SignalAccount::new(TelemetrySignal::Logs, MAX_QUEUE));
    let batch = BatchLogProcessor::builder(Counting::new(gate.clone(), account.clone()))
        .with_batch_config(
            opentelemetry_sdk::logs::BatchConfigBuilder::default()
                .with_max_queue_size((MAX_QUEUE + MAX_BATCH) as usize)
                .with_max_export_batch_size(MAX_BATCH as usize)
                .with_scheduled_delay(Duration::from_millis(20))
                .build(),
        )
        .build();
    let provider = SdkLoggerProvider::builder()
        .with_log_processor(GuardedLogProcessor::new(batch, account.clone()))
        .build();
    let logger = provider.logger("accounting");
    let emit = |n: usize| {
        for _ in 0..n {
            let mut record = logger.create_log_record();
            record.set_event_name("health.ready");
            logger.emit(record);
        }
    };
    gate.set_closed(true);
    emit(50);
    assert_eq!(account.counts().dropped_queue_full, 40);
    gate.set_closed(false);
    settle_until(&account, |c| c.pending == 0);
    gate.set_closed(true);
    emit(2);
    account.close();
    let _ = provider.shutdown_with_timeout(Duration::from_millis(100));
    account.finalize();
    gate.set_closed(false);
    let c = account.counts();
    assert_eq!(
        (
            c.emitted,
            c.exported,
            c.dropped_queue_full,
            c.dropped_shutdown,
            c.pending
        ),
        (52, 10, 40, 2, 0)
    );
    assert!(c.balanced());
}
