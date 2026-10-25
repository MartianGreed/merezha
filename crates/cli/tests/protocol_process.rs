//! A service declared `application_protocol = 2` whose guest only speaks
//! version 1 stops startup with `service_protocol_mismatch`, for `serve`
//! and for `merezha run` on PostgreSQL (AP §C.1, slice C1).
mod support;
use merezha_postgres::TestDatabase;
use std::{
    fs::{self, File},
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use support::Host;

const DATABASE_ENV: &str = "MEREZHA_PROTOCOL_TEST_DATABASE_URL";

/// The version 1 protocol component, declared as version 2.
fn runtime(dir: &Path) {
    fs::write(dir.join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.join("runtime.toml"),
        "version=1\n[limits]\nmemory_bytes=16777216\nfuel=1000000\nmax_input_bytes=65536\n\
         max_output_bytes=65536\ntimeout_ms=1000\nmax_concurrent_invocations=32\n\
         [[services]]\nid=\"protocol\"\npath=\"protocol.wasm\"\napplication_protocol=2\n",
    )
    .unwrap();
}
fn application(dir: &Path, storage: &str) {
    fs::write(
        dir.join("application.toml"),
        format!(
            "version=1\n{storage}\n[[commands]]\ntag=\"Add\"\nservice=\"protocol\"\naggregate=\"Counter\"\n"
        ),
    )
    .unwrap();
}

#[test]
fn serve_refuses_a_guest_that_does_not_speak_its_declared_protocol() {
    let dir = tempfile::tempdir().unwrap();
    runtime(dir.path());
    application(dir.path(), "");
    let log = dir.path().join("serve.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["serve", "--manifest"])
        .arg(dir.path().join("runtime.toml"))
        .arg("--application")
        .arg(dir.path().join("application.toml"))
        .args(["--listen", "127.0.0.1:0"])
        .stdout(Stdio::null())
        .stderr(File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "serve did not stop");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status.code(), Some(2));
    let failed = fs::read_to_string(&log)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|event| event["event"] == "startup.failed")
        .expect("startup.failed event");
    assert_eq!(failed["code"], "service_protocol_mismatch");
}

#[test]
fn run_refuses_a_guest_that_does_not_speak_its_declared_protocol() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    runtime(dir.path());
    application(
        dir.path(),
        "[storage]\nbackend=\"postgres\"\napplication=\"protocol-run\"",
    );
    let config = dir.path().join("merezha.toml");
    fs::write(
        &config,
        format!(
            "[app]\nid = \"protocol-run\"\n[http]\nlisten = \"127.0.0.1:0\"\n\
             [database]\nurl = {{ env = \"{DATABASE_ENV}\" }}\nmigrate = true\npool_size = 4\n\
             [runtime]\nmanifest = \"runtime.toml\"\n[application]\npath = \"application.toml\"\n"
        ),
    )
    .unwrap();
    let failed = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "run").failed();
    assert_eq!(failed["code"], "service_protocol_mismatch");
}
