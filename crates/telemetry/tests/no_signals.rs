//! With `signals = []` no OpenTelemetry thread starts (TE §3, T1): the
//! stderr writer is the only thread the telemetry block adds. Its own test
//! binary, so no other test starts threads meanwhile.
// The pipeline's own tests drive shutdown and emitters on raw blocking
// threads: the ban on `spawn_blocking` (clippy.toml) exempts this crate.
#![allow(clippy::disallowed_methods)]
use merezha_telemetry::{Signal, Telemetry, TelemetryConfig, config::Service};

#[cfg(target_os = "linux")]
fn threads() -> Vec<String> {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|task| std::fs::read_to_string(task.ok()?.path().join("comm")).ok())
        .map(|name| name.trim().to_owned())
        .collect()
}
#[cfg(target_os = "macos")]
fn threads() -> Vec<String> {
    // SAFETY: proc_pidinfo writes at most `size` bytes into `info`.
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as i32;
    let written = unsafe {
        libc::proc_pidinfo(
            std::process::id() as i32,
            libc::PROC_PIDTASKINFO,
            0,
            (&mut info as *mut libc::proc_taskinfo).cast(),
            size,
        )
    };
    assert_eq!(written, size);
    vec![String::new(); info.pti_threadnum as usize]
}

#[test]
fn no_signals_start_no_opentelemetry_threads() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let before = threads();
        let config = TelemetryConfig {
            signals: Vec::new(),
            ..TelemetryConfig::default()
        };
        let telemetry = Telemetry::init(&config, &Service::raw()).unwrap();
        assert!(!telemetry.exporting());
        tracing::info_span!(target: "merezha::test", "startup").in_scope(|| {
            merezha_telemetry::event!(INFO, "health.ready");
        });
        let after = threads();
        assert_eq!(
            after.len(),
            before.len() + 1,
            "only the stderr writer: {after:?}"
        );
        #[cfg(target_os = "linux")]
        {
            assert!(after.iter().any(|t| t == "merezha-stderr"), "{after:?}");
            assert!(
                !after.iter().any(|t| t.starts_with("OpenTelemetry")),
                "{after:?}"
            );
        }

        // Positive control: every signal starts the SDK's worker threads.
        let all = TelemetryConfig {
            signals: Signal::ALL.to_vec(),
            ..TelemetryConfig::default()
        };
        let (exporting, _dispatch) =
            Telemetry::build(&all, &Service::raw(), std::io::sink).unwrap();
        assert!(exporting.exporting());
        assert!(threads().len() >= after.len() + 3);
        #[cfg(target_os = "linux")]
        assert!(threads().iter().any(|t| t.starts_with("OpenTelemetry")));
        let _ = tokio::task::spawn_blocking(move || {
            exporting.shutdown();
            telemetry.shutdown();
        })
        .await;
    });
}
