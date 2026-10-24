//! Collector down (TE §8): through the real OTLP exporters on a
//! multi-thread runtime, for both protocols, against a blackhole listener
//! (accepts, never replies) and a closed port, 1 000 000 spans and 200 000
//! log events from 8 tasks while serving 10 000 HTTP requests. Telemetry
//! never blocks, memory stays bounded, accounting is exact, shutdown keeps
//! its budget and exports resume when a receiver appears.
// The pipeline's own tests drive shutdown and emitters on raw blocking
// threads: the ban on `spawn_blocking` (clippy.toml) exempts this crate.
#![allow(clippy::disallowed_methods)]
use axum::{Router, extract::State, routing::get};
use merezha_telemetry::{
    Signal, Telemetry, TelemetryConfig,
    config::{LogLevel, Protocol, Service},
};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::Dispatch;

const TASKS: usize = 8;
const SPANS: usize = 1_000_000;
const LOGS: usize = 200_000;
const REQUESTS: usize = 10_000;
/// Concurrent HTTP clients. Few, like the runtime's workers, so the
/// runnable threads stay close to a small runner's cores.
const CLIENTS: usize = 2;
const SHUTDOWN: Duration = Duration::from_millis(1_000);
const SCHEDULE: Duration = Duration::from_millis(1_000);
/// Emitters may use this share of the machine's cores: the test proves
/// telemetry never blocks a request, not that a saturated machine stays
/// fast. The pace is calibrated against the debug-build cost of a span.
const EMITTER_SHARE: f64 = 0.25;
const MIN_PACE: Duration = Duration::from_secs(3);
const MAX_PACE: Duration = Duration::from_secs(40);
/// A call slower than this counts as slow.
const SLOW_CALL: Duration = Duration::from_millis(1);
/// A call this slow is a stall.
const STALL_CALL: Duration = Duration::from_millis(20);
/// Baseline-and-run pairs a scenario may take to keep its tails.
const ATTEMPTS: usize = 3;
/// Largest excess of the slowest call over the baseline's slowest.
const STALL: Duration = Duration::from_millis(250);
/// Absolute slack on the HTTP p99 comparison.
const SLACK: Duration = Duration::from_millis(5);
/// No call may come near the 1 s export timeout.
const NEVER_WAITS: Duration = Duration::from_millis(500);

fn max_rss_bytes() -> u64 {
    // SAFETY: getrusage only writes the struct it is given.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    let raw = usage.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        raw
    } else {
        raw * 1024
    }
}

/// The production stderr path, lossy and non-blocking, into a sink.
fn stderr_sink() -> (
    tracing_appender::non_blocking::NonBlocking,
    tracing_appender::non_blocking::WorkerGuard,
) {
    tracing_appender::non_blocking::NonBlockingBuilder::default()
        .lossy(true)
        .buffered_lines_limit(8_192)
        .finish(std::io::sink())
}

fn config(protocol: Protocol, port: u16, signals: Vec<Signal>) -> TelemetryConfig {
    let mut config = TelemetryConfig {
        endpoint: match protocol {
            Protocol::HttpProtobuf => format!("http://127.0.0.1:{port}"),
            Protocol::Grpc => format!("http://127.0.0.1:{port}"),
        },
        protocol,
        timeout: Duration::from_millis(1_000),
        shutdown_timeout: SHUTDOWN,
        signals,
        log_level: LogLevel::Info,
        ..TelemetryConfig::default()
    };
    config.traces.schedule_delay = SCHEDULE;
    config.logs.schedule_delay = SCHEDULE;
    config.metrics.interval = Duration::from_millis(1_000);
    config
}

/// Accepts connections and never answers.
fn blackhole() -> (u16, std::thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let mut held = Vec::new();
        listener.set_nonblocking(true).unwrap();
        let until = Instant::now() + Duration::from_secs(600);
        while Instant::now() < until {
            match listener.accept() {
                Ok((socket, _)) => held.push(socket),
                Err(_) => std::thread::sleep(Duration::from_millis(5)),
            }
            if held.len() > 256 {
                held.remove(0);
            }
        }
    });
    (port, handle)
}
fn closed_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[derive(Clone)]
struct App {
    dispatch: Dispatch,
}
async fn handle(State(app): State<App>) -> &'static str {
    tracing::dispatcher::with_default(&app.dispatch, || {
        let span = tracing::info_span!(target: "merezha::test", "query {tag}");
        span.in_scope(|| merezha_telemetry::event!(INFO, "health.ready"));
    });
    "ok"
}

struct Run {
    slowest: Duration,
    /// Calls over [`SLOW_CALL`], reported only: scheduler noise.
    slow_calls: u64,
    /// Calls of at least [`STALL_CALL`], the bounded measure.
    stalls: u64,
    p99: Duration,
    emitted_spans: u64,
}

/// The workload: 8 blocking tasks emitting spans and logs while clients
/// send requests to a server whose handler also emits.
async fn workload(dispatch: Dispatch, pace: Duration) -> Run {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    let router = Router::new().route("/", get(handle)).with_state(App {
        dispatch: dispatch.clone(),
    });
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    let slowest = Arc::new(AtomicU64::new(0));
    let slow_calls = Arc::new(AtomicU64::new(0));
    let stall_calls = Arc::new(AtomicU64::new(0));
    // Emitters run for at least their share and for as long as requests
    // are served, so every run loads the CPU the same way during the HTTP
    // phase and the comparison with the baseline is fair.
    let serving = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let emitted = Arc::new(AtomicU64::new(0));
    let emitters: Vec<_> = (0..TASKS)
        .map(|_| {
            let dispatch = dispatch.clone();
            let (slowest, slow_calls) = (slowest.clone(), slow_calls.clone());
            let stall_calls = stall_calls.clone();
            let (serving, emitted) = (serving.clone(), emitted.clone());
            tokio::task::spawn_blocking(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    let mut worst = Duration::ZERO;
                    let mut slow = 0u64;
                    let mut stalls = 0u64;
                    let mut record = |i: usize, took: Duration| {
                        // The first call registers the callsite.
                        if i > 0 {
                            worst = worst.max(took);
                            slow += u64::from(took > SLOW_CALL);
                            stalls += u64::from(took >= STALL_CALL);
                        }
                    };
                    let mut i = 0;
                    let begun = Instant::now();
                    while i < SPANS / TASKS || serving.load(Ordering::Relaxed) {
                        // Paced: an application spends its CPU on requests,
                        // not on emitting telemetry.
                        if i % 256 == 0 {
                            let due = pace.mul_f64(i as f64 / (SPANS / TASKS) as f64);
                            if let Some(ahead) = due.checked_sub(begun.elapsed()) {
                                std::thread::sleep(ahead);
                            }
                        }
                        let span = tracing::info_span!(target: "merezha::test", "startup");
                        let started = Instant::now();
                        drop(span);
                        record(i, started.elapsed());
                        if i % (SPANS / LOGS) == 0 {
                            let started = Instant::now();
                            merezha_telemetry::event!(INFO, "health.ready");
                            record(i, started.elapsed());
                        }
                        i += 1;
                    }
                    emitted.fetch_add(i as u64, Ordering::Relaxed);
                    slowest.fetch_max(worst.as_nanos() as u64, Ordering::Relaxed);
                    slow_calls.fetch_add(slow, Ordering::Relaxed);
                    stall_calls.fetch_add(stalls, Ordering::Relaxed);
                })
            })
        })
        .collect();

    let client = reqwest::Client::new();
    let latencies: Vec<Duration> = futures_join(
        (0..CLIENTS)
            .map(|_| {
                let client = client.clone();
                tokio::spawn(async move {
                    let share = REQUESTS / CLIENTS;
                    let mut out = Vec::with_capacity(share);
                    let begun = Instant::now();
                    for i in 0..share {
                        // Paced like the emitters, so requests measure the
                        // request path, not a saturated machine.
                        let due = pace.mul_f64(i as f64 / share as f64);
                        if let Some(ahead) = due.checked_sub(begun.elapsed()) {
                            tokio::time::sleep(ahead).await;
                        }
                        let started = Instant::now();
                        let response = client
                            .get(format!("http://{address}/"))
                            .send()
                            .await
                            .unwrap();
                        assert!(response.status().is_success());
                        out.push(started.elapsed());
                    }
                    out
                })
            })
            .collect(),
    )
    .await;
    serving.store(false, Ordering::Relaxed);
    for emitter in emitters {
        emitter.await.unwrap();
    }
    server.abort();
    let mut latencies = latencies;
    latencies.sort();
    Run {
        slowest: Duration::from_nanos(slowest.load(Ordering::Relaxed)),
        slow_calls: slow_calls.load(Ordering::Relaxed),
        stalls: stall_calls.load(Ordering::Relaxed),
        p99: latencies[latencies.len() * 99 / 100],
        emitted_spans: emitted.load(Ordering::Relaxed) + REQUESTS as u64,
    }
}
async fn futures_join(handles: Vec<tokio::task::JoinHandle<Vec<Duration>>>) -> Vec<Duration> {
    let mut all = Vec::new();
    for handle in handles {
        all.extend(handle.await.unwrap());
    }
    all
}

/// The tail bounds, compared with the baseline run just before.
///
/// - Stalls: calls of at least 20 ms, at most five times the baseline's
///   plus 500, and the slowest call within 250 ms of the baseline's
///   slowest. CI runners see almost no stalls (slowest calls near 16 ms); a
///   developer machine at load average 30–60 saw up to about 320 and
///   slowest calls near 200 ms. The injected stall gives about 1 000.
///   Calls over 1 ms are reported, not bounded: on shared CI runners
///   scheduler preemption pushes 0.1–0.5% of full-pipeline calls past 1 ms
///   (a span through the pipeline is several times longer than a no-op one,
///   so it is preempted more often), while the slowest stays under 20 ms. A
///   stall in the pipeline itself (50 ms on one call in a thousand) gives
///   about a thousand stalls against a baseline of none.
/// - HTTP p99 within 4x the baseline, or within 5 ms of it: debug builds
///   pay the span, export and log path in the handler and on the SDK
///   threads, which CI runners measure at up to 3.5x a 1 ms baseline. A
///   request that waited on the stalled collector would take the 1 s export
///   timeout.
///
/// Requests and emitters are both paced, so neither run saturates the
/// machine.
fn tails(label: &str, run: &Run, baseline: &Run) -> Result<(), String> {
    let allowed = baseline.stalls * 5 + 500;
    if run.stalls > allowed {
        return Err(format!(
            "{label}: {} of {} span end or emit calls stalled {STALL_CALL:?} or more (baseline {})",
            run.stalls, run.emitted_spans, baseline.stalls
        ));
    }
    if run.slowest > baseline.slowest + STALL {
        return Err(format!(
            "{label}: the slowest span end or emit took {:?} (baseline {:?})",
            run.slowest, baseline.slowest
        ));
    }
    let allowed = (baseline.p99 * 4).max(baseline.p99 + SLACK);
    if run.p99 > allowed {
        return Err(format!(
            "{label}: HTTP p99 {:?} against baseline {:?}",
            run.p99, baseline.p99
        ));
    }
    Ok(())
}

/// One scenario: returns the tail comparison, asserts everything else.
async fn scenario(
    protocol: Protocol,
    port: u16,
    baseline: &Run,
    recover: bool,
    pace: Duration,
) -> Result<(), String> {
    let rss_before = max_rss_bytes();
    let config = config(protocol, port, Signal::ALL.to_vec());
    let (stderr, _guard) = stderr_sink();
    let (telemetry, dispatch) = Telemetry::build(&config, &Service::raw(), stderr).unwrap();
    let run = workload(dispatch.clone(), pace).await;
    let label = format!("{protocol:?} on port {port}");
    eprintln!(
        "{label}: p99 {:?} (baseline {:?}), slowest {:?} (baseline {:?}), slow calls {} (baseline {}), stalls {} (baseline {})",
        run.p99,
        baseline.p99,
        run.slowest,
        baseline.slowest,
        run.slow_calls,
        baseline.slow_calls,
        run.stalls,
        baseline.stalls
    );
    // Hard bound, every attempt: no call comes near the 1 s export
    // timeout a pipeline waiting on the stalled collector would show.
    assert!(
        run.slowest < NEVER_WAITS,
        "{label}: the slowest span end or emit took {:?}",
        run.slowest
    );
    let traces = telemetry.accounts().traces.clone();
    let counts = traces.counts();
    assert_eq!(counts.emitted, run.emitted_spans, "{label}");
    assert!(counts.balanced(), "{label}: {counts:?}");
    assert!(
        counts.dropped_queue_full > 0,
        "{label}: the bounded queue refused spans"
    );

    if recover {
        // A receiver starts on the port: exports resume within two
        // schedule delays.
        let received = Arc::new(AtomicU64::new(0));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        let counter = received.clone();
        let stub = Router::new().fallback(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            async {
                (
                    [("content-type", "application/x-protobuf")],
                    Vec::<u8>::new(),
                )
            }
        });
        let receiver = tokio::spawn(async move { axum::serve(listener, stub).await });
        let before = traces.counts().exported;
        let started = Instant::now();
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info_span!(target: "merezha::test", "startup").in_scope(|| {});
        });
        while traces.counts().exported <= before {
            assert!(
                started.elapsed() <= SCHEDULE * 2,
                "{label}: exports did not resume in {:?}: {:?}",
                started.elapsed(),
                traces.counts()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(received.load(Ordering::Relaxed) > 0);
        receiver.abort();
    }

    let started = Instant::now();
    let report = tokio::task::spawn_blocking(move || telemetry.shutdown())
        .await
        .unwrap();
    assert!(
        started.elapsed() <= SHUTDOWN + Duration::from_millis(500),
        "{label}: shutdown took {:?}",
        started.elapsed()
    );
    for (signal, counts) in &report.signals {
        assert!(counts.balanced(), "{label} {signal}: {counts:?}");
    }
    let growth = max_rss_bytes().saturating_sub(rss_before);
    assert!(
        growth < 32 * 1024 * 1024,
        "{label}: max RSS grew {growth} bytes"
    );
    tails(&label, &run, baseline)
}

/// How long the emitters spread their share over, so that they use about
/// [`EMITTER_SHARE`] of the cores: the single-threaded cost of spans and
/// events through the full pipeline, times the workload, over the budget.
async fn calibrate() -> Duration {
    let config = config(Protocol::HttpProtobuf, closed_port(), Signal::ALL.to_vec());
    let (stderr, _guard) = stderr_sink();
    let (telemetry, dispatch) = Telemetry::build(&config, &Service::raw(), stderr).unwrap();
    const SAMPLE: usize = 20_000;
    let took = tokio::task::spawn_blocking(move || {
        tracing::dispatcher::with_default(&dispatch, || {
            let started = Instant::now();
            for i in 0..SAMPLE {
                drop(tracing::info_span!(target: "merezha::test", "startup"));
                if i % (SPANS / LOGS) == 0 {
                    merezha_telemetry::event!(INFO, "health.ready");
                }
            }
            started.elapsed()
        })
    })
    .await
    .unwrap();
    let _ = tokio::task::spawn_blocking(move || telemetry.shutdown()).await;
    let cores = std::thread::available_parallelism().map_or(2, |n| n.get()) as f64;
    let cpu = took.mul_f64(SPANS as f64 / SAMPLE as f64);
    cpu.div_f64(cores * EMITTER_SHARE).clamp(MIN_PACE, MAX_PACE)
}

/// The same workload with `signals = []`, run right before each scenario so
/// both see the same machine.
async fn baseline(pace: Duration) -> Run {
    let config = config(Protocol::HttpProtobuf, closed_port(), Vec::new());
    let (stderr, _guard) = stderr_sink();
    let (telemetry, dispatch) = Telemetry::build(&config, &Service::raw(), stderr).unwrap();
    assert!(!telemetry.exporting());
    workload(dispatch, pace).await
}

#[test]
fn a_down_collector_never_blocks_and_never_grows_memory() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let pace = calibrate().await;
        eprintln!("pace {pace:?}");
        let (hole, _hole) = blackhole();
        for protocol in [Protocol::HttpProtobuf, Protocol::Grpc] {
            let recover = protocol == Protocol::HttpProtobuf;
            for (port, recover) in [(hole, false), (closed_port(), recover)] {
                // Timing tails are noisy on shared runners: a scenario
                // passes when one of three baseline-and-run pairs keeps
                // them. A pipeline that blocked on the collector fails all
                // three; the accounting, memory and shutdown checks hold
                // on every attempt.
                let mut failures = Vec::new();
                for _ in 0..ATTEMPTS {
                    let base = baseline(pace).await;
                    match scenario(protocol, port, &base, recover, pace).await {
                        Ok(()) => break,
                        Err(failure) => failures.push(failure),
                    }
                }
                assert!(failures.len() < ATTEMPTS, "{failures:#?}");
            }
        }
    });
}
