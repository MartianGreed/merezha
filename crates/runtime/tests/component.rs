use merezha_runtime::{Error, Runtime, manifest::Manifest};
use std::time::{Duration, Instant};

const HEADER: &str = r#"version = 1
[limits]
memory_bytes = 16777216
fuel = 10000000
max_input_bytes = 65536
max_output_bytes = 65536
timeout_ms = 1000
max_concurrent_invocations = 8
"#;
const ECHO: &str = "i32.const 0 i32.const 0 i32.store i32.const 4 local.get $p i32.store i32.const 8 local.get $n i32.store i32.const 0";
fn component(body: &str, extra: &str, options: &str) -> Vec<u8> {
    wat::parse_str(format!(
        r#"(component
      (core module $m
        (memory (export "memory") 1)
        (global $next (mut i32) (i32.const 1024))
        {extra}
        (func (export "realloc") (param i32 i32 i32 i32) (result i32)
          (local $p i32)
          global.get $next local.tee $p
          local.get 3 i32.add global.set $next local.get $p)
        (func (export "handle") (param $p i32) (param $n i32) (result i32) {body}))
      (core instance $i (instantiate $m))
      (type $h (func (param "input" string) (result (result string (error string)))))
      (func $handle (type $h) (canon lift (core func $i "handle")
        (memory (core memory $i "memory")) (realloc (core func $i "realloc")) {options}))
      (instance $handler (export "handle" (func $handle)))
      (export "merezha:service/handler@0.1.0" (instance $handler)))"#
    ))
    .unwrap()
}
fn echo() -> Vec<u8> {
    component(ECHO, "", "")
}
fn constant(value: u8) -> Vec<u8> {
    component(
        &format!(
            "i32.const 64 i32.const {value} i32.store8 i32.const 0 i32.const 0 i32.store i32.const 4 i32.const 64 i32.store i32.const 8 i32.const 1 i32.store i32.const 0"
        ),
        "",
        "",
    )
}
struct Fixture {
    dir: tempfile::TempDir,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new(bytes: &[u8], header: &str, rest: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("echo.wasm"), bytes).unwrap();
        let path = dir.path().join("runtime.toml");
        std::fs::write(
            &path,
            format!("{header}[[services]]\nid = \"echo\"\npath = \"echo.wasm\"\n{rest}"),
        )
        .unwrap();
        Self { dir, path }
    }
    fn runtime(&self) -> Runtime {
        Runtime::load(&self.path).unwrap()
    }
}
#[test]
fn real_component_round_trips_opaque_payload() {
    let f = Fixture::new(&echo(), HEADER, "");
    assert_eq!(
        f.runtime()
            .invoke_service("echo", r#" {"name":"é"} "#)
            .unwrap()
            .output,
        r#" {"name":"é"} "#
    );
}
#[test]
fn ordered_pipeline_passes_each_success_to_next_step() {
    let append = "local.get $p local.get $n i32.add i32.const 33 i32.store8 i32.const 0 i32.const 0 i32.store i32.const 4 local.get $p i32.store i32.const 8 local.get $n i32.const 1 i32.add i32.store i32.const 0";
    let f = Fixture::new(
        &component(append, "", ""),
        HEADER,
        "[[pipelines]]\nid = \"twice\"\nsteps = [\"echo\", \"echo\"]\n",
    );
    assert_eq!(
        f.runtime().invoke_pipeline("twice", "hi").unwrap().output,
        "hi!!"
    );
}
#[test]
fn add_replace_remove_and_failed_reload_preserve_snapshots() {
    let f = Fixture::new(
        &constant(b'A'),
        HEADER,
        "[[pipelines]]\nid = \"twice\"\nsteps = [\"echo\", \"echo\"]\n",
    );
    let rt = f.runtime();
    let old = rt.prepare_pipeline("twice").unwrap();
    std::fs::write(f.dir.path().join("echo.wasm"), constant(b'B')).unwrap();
    assert_eq!(rt.reload().unwrap(), 2);
    assert_eq!(old.run("").unwrap().output, "A");
    assert_eq!(rt.invoke_service("echo", "").unwrap().output, "B");
    std::fs::write(f.dir.path().join("new.wasm"), echo()).unwrap();
    let text = std::fs::read_to_string(&f.path).unwrap();
    std::fs::write(
        &f.path,
        format!("{text}[[services]]\nid = \"new\"\npath = \"new.wasm\"\n"),
    )
    .unwrap();
    assert_eq!(rt.reload().unwrap(), 3);
    assert_eq!(rt.invoke_service("new", "added").unwrap().output, "added");
    std::fs::write(f.dir.path().join("echo.wasm"), b"broken").unwrap();
    assert!(rt.reload().is_err());
    assert_eq!(rt.catalog().unwrap().generation, 3);
    assert_eq!(rt.invoke_service("echo", "").unwrap().output, "B");
    std::fs::write(&f.path, HEADER).unwrap();
    assert_eq!(rt.reload().unwrap(), 4);
    assert_eq!(rt.invoke_service("echo", "").unwrap_err(), Error::NotFound);
    assert!(rt.catalog().unwrap().services.is_empty());
}
#[test]
fn concurrent_reloads_have_distinct_monotonic_generations() {
    let f = Fixture::new(&echo(), HEADER, "");
    let rt = f.runtime();
    let joins: Vec<_> = (0..4)
        .map(|_| {
            let rt = rt.clone();
            std::thread::spawn(move || rt.reload().unwrap())
        })
        .collect();
    let mut generations: Vec<_> = joins.into_iter().map(|j| j.join().unwrap()).collect();
    generations.sort();
    assert_eq!(generations, [2, 3, 4, 5]);
    assert_eq!(rt.catalog().unwrap().generation, 5);
}
#[test]
fn stores_are_fresh_and_guest_globals_do_not_survive() {
    let body = "global.get $counter i32.const 1 i32.add global.set $counter i32.const 64 global.get $counter i32.const 48 i32.add i32.store8 i32.const 0 i32.const 0 i32.store i32.const 4 i32.const 64 i32.store i32.const 8 i32.const 1 i32.store i32.const 0";
    let f = Fixture::new(
        &component(body, "(global $counter (mut i32) (i32.const 0))", ""),
        HEADER,
        "[[pipelines]]\nid=\"twice\"\nsteps=[\"echo\",\"echo\"]\n",
    );
    let rt = f.runtime();
    for _ in 0..3 {
        assert_eq!(rt.invoke_pipeline("twice", "").unwrap().output, "1");
    }
}
#[test]
fn guest_error_trap_and_memory_exhaustion_do_not_kill_host() {
    for (body, expected) in [
        (
            ECHO.replacen(
                "i32.const 0 i32.const 0 i32.store",
                "i32.const 0 i32.const 1 i32.store",
                1,
            ),
            Error::GuestError,
        ),
        ("unreachable".to_owned(), Error::Trap),
        (
            format!("i32.const 1024 memory.grow drop {ECHO}"),
            Error::MemoryLimit,
        ),
    ] {
        let f = Fixture::new(&component(&body, "", ""), HEADER, "");
        let rt = f.runtime();
        assert_eq!(rt.invoke_service("echo", "secret").unwrap_err(), expected);
        std::fs::write(f.dir.path().join("echo.wasm"), echo()).unwrap();
        rt.reload().unwrap();
        assert_eq!(rt.invoke_service("echo", "alive").unwrap().output, "alive");
    }
}
#[test]
fn fuel_and_wall_clock_deadline_interrupt_real_infinite_guest() {
    let bytes = component("(loop $forever br $forever) unreachable", "", "");
    for (header, expected) in [
        (
            HEADER.replace("fuel = 10000000", "fuel = 1000"),
            Error::FuelExhausted,
        ),
        (
            HEADER
                .replace("fuel = 10000000", "fuel = 1000000000")
                .replace("timeout_ms = 1000", "timeout_ms = 5"),
            Error::Timeout,
        ),
    ] {
        let f = Fixture::new(&bytes, &header, "");
        let rt = f.runtime();
        let start = Instant::now();
        assert_eq!(rt.invoke_service("echo", "").unwrap_err(), expected);
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
#[test]
fn pipeline_fuel_is_a_single_budget() {
    let f = Fixture::new(
        &echo(),
        &HEADER.replace("fuel = 10000000", "fuel = 100"),
        "[[pipelines]]\nid=\"many\"\nsteps=[\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\",\"echo\"]\n",
    );
    let rt = f.runtime();
    assert!(rt.invoke_service("echo", "a").is_ok());
    assert_eq!(
        rt.invoke_pipeline("many", "a").unwrap_err(),
        Error::FuelExhausted
    );
}
#[test]
fn pipeline_deadline_starts_at_admission_and_is_not_reset() {
    let f = Fixture::new(
        &echo(),
        &HEADER.replace("timeout_ms = 1000", "timeout_ms = 5"),
        "[[pipelines]]\nid=\"twice\"\nsteps=[\"echo\",\"echo\"]\n",
    );
    let invocation = f.runtime().prepare_pipeline("twice").unwrap();
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(invocation.run("a").unwrap_err(), Error::Timeout);
}
#[test]
fn input_and_success_and_guest_error_output_byte_limits() {
    let header = HEADER
        .replace("max_input_bytes = 65536", "max_input_bytes = 4")
        .replace("max_output_bytes = 65536", "max_output_bytes = 3");
    let f = Fixture::new(&echo(), &header, "");
    let rt = f.runtime();
    assert_eq!(
        rt.invoke_service("echo", "ééé").unwrap_err(),
        Error::InputLimit
    );
    assert_eq!(
        rt.invoke_service("echo", "éé").unwrap_err(),
        Error::OutputLimit
    );
    assert_eq!(rt.invoke_service("echo", "é").unwrap().output, "é");
    let body = ECHO.replacen(
        "i32.const 0 i32.const 0 i32.store",
        "i32.const 0 i32.const 1 i32.store",
        1,
    );
    std::fs::write(f.dir.path().join("echo.wasm"), component(&body, "", "")).unwrap();
    rt.reload().unwrap();
    assert_eq!(
        rt.invoke_service("echo", "four").unwrap_err(),
        Error::OutputLimit
    );
}
#[test]
fn output_is_copied_before_post_return_overwrites_guest_memory() {
    let body = "i32.const 64 i32.const 65 i32.store8 i32.const 0 i32.const 0 i32.store i32.const 4 i32.const 64 i32.store i32.const 8 i32.const 1 i32.store i32.const 0";
    let extra = "(func (export \"post\") (param i32) i32.const 64 i32.const 90 i32.store8)";
    let f = Fixture::new(
        &component(body, extra, "(post-return (core func $i \"post\"))"),
        HEADER,
        "",
    );
    assert_eq!(f.runtime().invoke_service("echo", "").unwrap().output, "A");
}
#[test]
fn invalid_utf8_and_out_of_bounds_result_are_contained() {
    for body in [
        "i32.const 64 i32.const 255 i32.store8 i32.const 0 i32.const 0 i32.store i32.const 4 i32.const 64 i32.store i32.const 8 i32.const 1 i32.store i32.const 0",
        "i32.const 0 i32.const 0 i32.store i32.const 4 i32.const 65530 i32.store i32.const 8 i32.const 100 i32.store i32.const 0",
    ] {
        let f = Fixture::new(&component(body, "", ""), HEADER, "");
        assert_eq!(
            f.runtime().invoke_service("echo", "").unwrap_err(),
            Error::Trap
        );
    }
}
#[test]
fn runtime_admission_is_shared_and_released_after_errors_and_drop() {
    let f = Fixture::new(
        &echo(),
        &HEADER.replace(
            "max_concurrent_invocations = 8",
            "max_concurrent_invocations = 1",
        ),
        "",
    );
    let rt = f.runtime();
    let first = rt.prepare_service("echo").unwrap();
    assert!(
        f.runtime()
            .invoke_service("echo", "separate runtime")
            .is_ok()
    );
    assert_eq!(
        rt.clone().invoke_service("echo", "").unwrap_err(),
        Error::Overloaded
    );
    drop(first);
    assert_eq!(rt.invoke_service("echo", "a").unwrap().output, "a");
    let first = rt.prepare_service("echo").unwrap();
    assert_eq!(
        first.run(&"x".repeat(65537)).unwrap_err(),
        Error::InputLimit
    );
    assert!(rt.invoke_service("echo", "").is_ok());
}
#[test]
fn manifest_rejects_unknown_keys_duplicate_ids_references_and_ranges() {
    let valid = format!("{HEADER}[[services]]\nid=\"echo\"\npath=\"echo.wasm\"\n");
    for invalid in [
        valid.replace("version = 1", "version = 2"),
        format!("unknown = true\n{valid}"),
        valid.replace("[limits]", "[limits]\nunknown=1"),
        format!("{valid}unknown=1\n"),
        format!("{valid}[[services]]\nid=\"echo\"\npath=\"x.wasm\"\n"),
        format!("{valid}[[pipelines]]\nid=\"p\"\nsteps=[\"missing\"]\n"),
        format!("{valid}[[pipelines]]\nid=\"p\"\nsteps=[]\n"),
        format!("{valid}[[pipelines]]\nid=\"p\"\nsteps=[\"echo\"]\nunknown=true\n"),
        valid.replace("path=\"echo.wasm\"", "path=\"../echo.wasm\""),
        valid.replace("path=\"echo.wasm\"", "path=\"/echo.wasm\""),
        valid.replace("id=\"echo\"", "id=\"bad/id\""),
        valid.replace("fuel = 10000000", "fuel = 0"),
        valid.replace("memory_bytes = 16777216", "memory_bytes = 65537"),
        valid.replace("timeout_ms = 1000", "timeout_ms = 30001"),
        valid.replace(
            "max_concurrent_invocations = 8",
            "max_concurrent_invocations = 65",
        ),
        valid.replace("max_output_bytes = 65536", "max_output_bytes = 0"),
    ] {
        assert!(Manifest::parse(&invalid).is_err(), "accepted {invalid}");
    }
}
#[test]
fn loader_rejects_noncomponents_imports_abi_and_transcoding_before_activation() {
    let import = wat::parse_str("(component (import \"probe\" (func)))").unwrap();
    for bytes in [
        b"native artifact".to_vec(),
        wat::parse_str("(module)").unwrap(),
        import,
        component(ECHO, "", "string-encoding=utf16"),
        {
            let mut s = echo();
            let pos = s.windows(7).position(|b| b == b"handler").unwrap();
            s[pos] = b'x';
            s
        },
    ] {
        let f = Fixture::new(&bytes, HEADER, "");
        assert!(Runtime::load(&f.path).is_err());
    }
}
#[test]
fn bounded_loader_rejects_oversized_manifest_component_and_initial_memory() {
    let f = Fixture::new(&echo(), HEADER, "");
    std::fs::write(&f.path, " ".repeat(65537)).unwrap();
    let error = Runtime::load(&f.path)
        .err()
        .expect("manifest must fail at the bounded read before parsing");
    assert!(error.to_string().contains("artifact byte limit"));
    let f = Fixture::new(&echo(), HEADER, "");
    std::fs::OpenOptions::new()
        .write(true)
        .open(f.dir.path().join("echo.wasm"))
        .unwrap()
        .set_len(16_777_217)
        .unwrap();
    assert!(Runtime::load(&f.path).is_err());
    let f = Fixture::new(
        &echo(),
        &HEADER.replace("memory_bytes = 16777216", "memory_bytes = 65536"),
        "",
    );
    let large = wat::parse_str(
        include_str!("fixtures/echo.wat").replace("(export \"memory\") 1", "(export \"memory\") 2"),
    )
    .unwrap();
    std::fs::write(f.dir.path().join("echo.wasm"), large).unwrap();
    assert!(Runtime::load(&f.path).is_err());
}

#[test]
fn pipeline_rechecks_intermediate_input_and_stops_on_first_guest_error() {
    let append = "local.get $p local.get $n i32.add i32.const 33 i32.store8 i32.const 0 i32.const 0 i32.store i32.const 4 local.get $p i32.store i32.const 8 local.get $n i32.const 1 i32.add i32.store i32.const 0";
    let f = Fixture::new(
        &component(append, "", ""),
        &HEADER.replace("max_input_bytes = 65536", "max_input_bytes = 1"),
        "[[pipelines]]\nid=\"twice\"\nsteps=[\"echo\",\"echo\"]\n",
    );
    assert_eq!(
        f.runtime().invoke_pipeline("twice", "a").unwrap_err(),
        Error::InputLimit
    );
    let error = ECHO.replacen(
        "i32.const 0 i32.const 0 i32.store",
        "i32.const 0 i32.const 1 i32.store",
        1,
    );
    let f = Fixture::new(
        &component(&error, "", ""),
        HEADER,
        "[[services]]\nid=\"trap\"\npath=\"trap.wasm\"\n[[pipelines]]\nid=\"stop\"\nsteps=[\"echo\",\"trap\"]\n",
    );
    std::fs::write(
        f.dir.path().join("trap.wasm"),
        component("unreachable", "", ""),
    )
    .unwrap();
    assert_eq!(
        f.runtime().invoke_pipeline("stop", "a").unwrap_err(),
        Error::GuestError
    );
}

#[test]
fn output_lift_limit_is_scoped_to_each_call_and_thread() {
    let small = Fixture::new(
        &echo(),
        &HEADER.replace("max_output_bytes = 65536", "max_output_bytes = 1"),
        "",
    );
    let large = Fixture::new(&echo(), HEADER, "");
    let small = small.runtime();
    let large = large.runtime();
    assert_eq!(
        small.invoke_service("echo", "abcd").unwrap_err(),
        Error::OutputLimit
    );
    assert_eq!(large.invoke_service("echo", "abcd").unwrap().output, "abcd");
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let small = small.clone();
            let large = large.clone();
            std::thread::spawn(move || {
                for _ in 0..10 {
                    assert_eq!(
                        small.invoke_service("echo", "abcd").unwrap_err(),
                        Error::OutputLimit
                    );
                    assert_eq!(large.invoke_service("echo", "abcd").unwrap().output, "abcd");
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn bounded_initialization_and_table_growth_fail_before_activation_or_during_call() {
    let source = include_str!("fixtures/echo.wat");
    let start_loop = source.replace(
        "(global $next",
        "(func $start (loop $forever br $forever)) (start $start) (global $next",
    );
    let f = Fixture::new(
        &wat::parse_str(start_loop).unwrap(),
        &HEADER.replace("fuel = 10000000", "fuel = 1000"),
        "",
    );
    let started = Instant::now();
    assert!(Runtime::load(&f.path).is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
    let body = format!("ref.null func i32.const 10001 table.grow drop {ECHO}");
    let f = Fixture::new(&component(&body, "(table 1 funcref)", ""), HEADER, "");
    assert_eq!(
        f.runtime().invoke_service("echo", "").unwrap_err(),
        Error::MemoryLimit
    );
}

#[test]
#[cfg(unix)]
fn loader_rejects_symlink_escape_and_nonregular_component_files() {
    use std::os::unix::fs::symlink;
    let f = Fixture::new(&echo(), HEADER, "");
    let external = tempfile::tempdir().unwrap();
    std::fs::write(external.path().join("outside.wasm"), echo()).unwrap();
    std::fs::remove_file(f.dir.path().join("echo.wasm")).unwrap();
    symlink(
        external.path().join("outside.wasm"),
        f.dir.path().join("echo.wasm"),
    )
    .unwrap();
    assert!(Runtime::load(&f.path).is_err());
    std::fs::remove_file(f.dir.path().join("echo.wasm")).unwrap();
    std::fs::create_dir(f.dir.path().join("echo.wasm")).unwrap();
    assert!(Runtime::load(&f.path).is_err());
}

#[test]
fn total_component_bytes_are_bounded_even_when_paths_repeat() {
    let bytes = padded_component(14 * 1024 * 1024);
    let f = Fixture::new(&bytes, HEADER, "");
    let rt = f.runtime();
    assert!(rt.invoke_service("echo", "ok").is_ok());
    let mut text = std::fs::read_to_string(&f.path).unwrap();
    for i in 0..4 {
        text.push_str(&format!(
            "[[services]]\nid=\"copy-{i}\"\npath=\"echo.wasm\"\n"
        ));
    }
    std::fs::write(&f.path, text).unwrap();
    assert!(rt.reload().is_err());
    assert_eq!(rt.catalog().unwrap().generation, 1);
}

#[test]
fn component_compile_failure_rejects_the_entire_initial_and_reloaded_catalog() {
    // This has the component header and parses structurally. Its core function
    // is invalid only at compilation because i64 cannot satisfy an i32 result.
    let invalid = component("i64.const 0", "", "");
    assert!(invalid.starts_with(b"\0asm\x0d\0\x01\0"));
    assert!(wasmtime::component::Component::new(&wasmtime::Engine::default(), &invalid).is_err());
    let f = Fixture::new(&echo(), HEADER, "");
    let rt = f.runtime();
    std::fs::write(f.dir.path().join("invalid.wasm"), invalid).unwrap();
    std::fs::write(f.dir.path().join("echo.wasm"), constant(b'B')).unwrap();
    let manifest = std::fs::read_to_string(&f.path).unwrap();
    std::fs::write(
        &f.path,
        format!("{manifest}[[services]]\nid=\"invalid\"\npath=\"invalid.wasm\"\n"),
    )
    .unwrap();
    let initial_error = Runtime::load(&f.path)
        .err()
        .expect("compile failure must abort the entire initial load");
    assert!(initial_error.to_string().contains("component compilation"));
    let reload_error = rt
        .reload()
        .expect_err("compile failure must abort the entire reload");
    assert!(reload_error.to_string().contains("component compilation"));
    let catalog = rt.catalog().unwrap();
    assert_eq!(catalog.generation, 1);
    assert_eq!(catalog.services, ["echo"]);
    assert_eq!(
        rt.invoke_service("echo", "old registry").unwrap().output,
        "old registry"
    );
}

#[test]
fn a_guest_that_runs_after_partial_admission_wait_keeps_the_original_deadline() {
    let f = Fixture::new(
        &component("(loop $forever br $forever) unreachable", "", ""),
        &HEADER
            .replace("fuel = 10000000", "fuel = 1000000000")
            .replace("timeout_ms = 1000", "timeout_ms = 600"),
        "[[pipelines]]\nid=\"looping\"\nsteps=[\"echo\",\"echo\"]\n",
    );
    let rt = f.runtime();
    let admitted = Instant::now();
    let invocation = rt.prepare_pipeline("looping").unwrap();
    std::thread::sleep(Duration::from_millis(400));
    assert!(
        Instant::now() < invocation.deadline(),
        "guest must begin before the admission deadline"
    );
    assert_eq!(invocation.run("").unwrap_err(), Error::Timeout);
    // The guest has about 200 ms left, with 250 ms scheduling tolerance. A
    // reset to a fresh 600 ms store budget would take about 1000 ms in total.
    assert!(
        admitted.elapsed() < Duration::from_millis(850),
        "guest execution reset the admission deadline: {:?}",
        admitted.elapsed()
    );
}

#[test]
fn manifest_accepts_count_boundaries_and_rejects_the_next_entry() {
    let services = |count| {
        let mut text = HEADER.to_owned();
        for n in 0..count {
            text.push_str(&format!("[[services]]\nid=\"s{n}\"\npath=\"echo.wasm\"\n"));
        }
        text
    };
    assert!(Manifest::parse(&services(64)).is_ok());
    assert!(Manifest::parse(&services(65)).is_err());
    let pipelines = |count| {
        let mut text = services(1);
        for n in 0..count {
            text.push_str(&format!("[[pipelines]]\nid=\"p{n}\"\nsteps=[\"s0\"]\n"));
        }
        text
    };
    assert!(Manifest::parse(&pipelines(64)).is_ok());
    assert!(Manifest::parse(&pipelines(65)).is_err());
    let steps = |count| {
        format!(
            "{}[[pipelines]]\nid=\"p\"\nsteps=[{}]\n",
            services(1),
            vec!["\"s0\""; count].join(",")
        )
    };
    assert!(Manifest::parse(&steps(16)).is_ok());
    assert!(Manifest::parse(&steps(17)).is_err());
}

#[test]
fn oversized_valid_component_is_rejected_by_its_own_byte_bound() {
    let bytes = padded_component(17 * 1024 * 1024);
    // The artifact is valid and below the aggregate registry byte bound. Its
    // rejection must depend on the per-component loader bound alone.
    assert!(wasmtime::component::Component::new(&wasmtime::Engine::default(), &bytes).is_ok());
    let f = Fixture::new(&bytes, HEADER, "");
    let error = Runtime::load(&f.path)
        .err()
        .expect("valid oversized component must not activate");
    assert!(error.to_string().contains("artifact byte limit"));
}

fn padded_component(size: usize) -> Vec<u8> {
    let mut bytes = echo();
    // A valid ignored custom section. It has no code or data for the compiler
    // to execute, but must still count against portable-artifact load limits.
    bytes.push(0);
    let mut length = size + 1;
    loop {
        let mut b = (length & 0x7f) as u8;
        length >>= 7;
        if length > 0 {
            b |= 0x80;
        }
        bytes.push(b);
        if length == 0 {
            break;
        }
    }
    bytes.push(0);
    bytes.resize(bytes.len() + size, 0);
    bytes
}
