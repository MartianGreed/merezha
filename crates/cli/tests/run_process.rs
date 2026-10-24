//! `merezha run` as a real process over loopback HTTP against real
//! PostgreSQL. Tests needing a database create one through
//! `MEREZHA_TEST_DATABASE_URL` and skip with a message when it is unset.
mod support;
use merezha_postgres::{Database, TestDatabase};
use serde_json::json;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};
use support::{Host, request};

const DATABASE_ENV: &str = "RUN_TEST_DATABASE_URL";

/// A project directory with the runtime, application and `merezha.toml`.
fn project(dir: &Path, extra: &str) -> PathBuf {
    fs::write(dir.join("protocol.wasm"), support::component()).unwrap();
    fs::write(
        dir.join("echo.wasm"),
        wat::parse_str(include_str!("../../runtime/tests/fixtures/echo.wat")).unwrap(),
    )
    .unwrap();
    fs::write(
        dir.join("runtime.toml"),
        r#"version=1
[limits]
memory_bytes=16777216
fuel=1000000
max_input_bytes=65536
max_output_bytes=65536
timeout_ms=1000
max_concurrent_invocations=4
[[services]]
id="protocol"
path="protocol.wasm"
[[services]]
id="echo"
path="echo.wasm"
"#,
    )
    .unwrap();
    fs::write(
        dir.join("application.toml"),
        r#"version=1
[[commands]]
tag="Add"
service="protocol"
aggregate="Counter"
[[queries]]
tag="Total"
service="protocol"
projection="total"
[[projections]]
name="total"
service="protocol"
initial=0
revision=1
"#,
    )
    .unwrap();
    let path = dir.join("merezha.toml");
    fs::write(
        &path,
        format!(
            r#"[app]
id = "run-process"
[http]
listen = "127.0.0.1:0"
[database]
url = {{ env = "{DATABASE_ENV}" }}
migrate = true
pool_size = 2
connect_timeout_ms = 2000
[runtime]
manifest = "runtime.toml"
[application]
path = "application.toml"
{extra}"#
        ),
    )
    .unwrap();
    path
}
const COMMAND: &str = r#"{"tag":"Add","stream":"one","payload":{"amount":1},"idempotency_key":"key","metadata":{"message_id":"m1","correlation_id":"c1","actor":"alice"}}"#;

fn ledger_exists(test: &TestDatabase) -> bool {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let db = Database::connect(&test.config()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let client = db.acquire(deadline).await.unwrap();
        client
            .query_one("SELECT to_regclass('merezha.migrations') IS NOT NULL", &[])
            .await
            .unwrap()
            .get(0)
    })
}

#[test]
fn run_serves_health_runtime_and_application_behind_the_edge_and_stops_gracefully() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "");
    let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "host");
    let address = host.ready();
    assert!(ledger_exists(&test), "migrations ran against PostgreSQL");

    let live = request(address, "GET", "/health/live", "");
    assert_eq!((live.status, live.body), (200, json!({"status":"live"})));
    let ready = request(address, "GET", "/health/ready", "");
    assert_eq!((ready.status, ready.body), (200, json!({"status":"ready"})));
    assert!(ready.headers.contains("x-request-id: "));

    let echo = request(address, "POST", "/v1/services/echo/invoke", "opaque");
    assert_eq!((echo.status, echo.body), (200, json!("opaque")));
    let receipt = request(address, "POST", "/v1/application/commands", COMMAND);
    assert_eq!(receipt.status, 200, "{}", receipt.body);
    assert_eq!(receipt.body["position"], 1);
    // A private listener keeps projection administration open.
    let caught_up = request(
        address,
        "POST",
        "/v1/application/projections/total/catch-up",
        "",
    );
    assert_eq!(
        (caught_up.status, caught_up.body),
        (200, json!({"state":1,"checkpoint":1}))
    );

    // One error shape for kernel, runtime and application failures.
    for (method, path, body, status, code) in [
        ("GET", "/missing", "", 404, "not_found"),
        ("POST", "/v1/services/missing/invoke", "x", 404, "not_found"),
        (
            "POST",
            "/v1/application/commands",
            "{",
            422,
            "validation_failed",
        ),
    ] {
        let reply = request(address, method, path, body);
        assert_eq!(reply.status, status, "{path}: {}", reply.body);
        assert_eq!(reply.body["error"]["code"], code, "{path}");
        assert_eq!(reply.body["error"]["classification"], "permanent", "{path}");
    }

    // Readiness follows the database while liveness does not.
    test.admin_execute(&format!(
        "ALTER DATABASE \"{0}\" ALLOW_CONNECTIONS false; SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{0}'",
        test.name()
    ));
    host.wait_for_readiness(address, 503);
    assert_eq!(request(address, "GET", "/health/live", "").status, 200);
    test.admin_execute(&format!(
        "ALTER DATABASE \"{}\" ALLOW_CONNECTIONS true",
        test.name()
    ));
    host.wait_for_readiness(address, 200);

    host.stop();
    let events: Vec<String> = host
        .events()
        .iter()
        .filter_map(|e| e["event"].as_str().map(str::to_owned))
        .collect();
    for expected in [
        "db.connected",
        "migration.checked",
        "listener.ready",
        "http.request.completed",
        "listener.draining",
        "listener.stopped",
    ] {
        assert!(
            events.iter().any(|e| e == expected),
            "missing {expected} in {events:?}"
        );
    }
    let log = fs::read_to_string(&host.log).unwrap();
    assert!(!log.contains(test.url()), "the database URL was logged");
}

#[test]
fn a_public_listener_without_identity_denies_application_and_hides_the_raw_runtime() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    const QUERY: &str =
        r#"{"tag":"Total","payload":{},"metadata":{"message_id":"q1","correlation_id":"c1"}}"#;
    // Both stay bound to loopback so the test exposes nothing. A trusted
    // proxy makes a loopback listener reachable from elsewhere, so it is
    // public too.
    for (name, setting) in [
        ("public", ("MEREZHA_HTTP__PUBLIC", "true")),
        ("proxied", ("MEREZHA_HTTP__TRUSTED_PROXIES", "127.0.0.1")),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = project(dir.path(), "");
        let mut host = Host::spawn(&config, &[(DATABASE_ENV, test.url()), setting], name);
        let address = host.listening();
        for path in ["/v1/services/echo/invoke", "/v1/reload", "/health"] {
            let method = if path == "/health" { "GET" } else { "POST" };
            let raw = request(address, method, path, "");
            assert_eq!(raw.status, 404, "{name} {path}: {}", raw.body);
        }
        // Messages carry no principal of their own: the policy refuses them.
        // Projection controls are permission checks, and an anonymous caller
        // is unauthenticated.
        for (path, body, status, code) in [
            ("/v1/application/commands", COMMAND, 403, "unauthorized"),
            ("/v1/application/queries", QUERY, 403, "unauthorized"),
            (
                "/v1/application/projections/total/catch-up",
                "",
                401,
                "unauthenticated",
            ),
            (
                "/v1/application/projections/total/rebuild",
                "",
                401,
                "unauthenticated",
            ),
            // The jobs admin API (AP-B2) is a permission check too.
            ("/v1/jobs/admin/dead/purge", "{}", 401, "unauthenticated"),
        ] {
            let denied = request(address, "POST", path, body);
            assert_eq!(
                (denied.status, denied.body),
                (
                    status,
                    json!({"error":{"code":code,"classification":"permanent"}})
                ),
                "{name} {path}"
            );
        }
        for listed in ["/v1/jobs/admin/queues", "/v1/jobs/admin/schedules"] {
            let listing = request(address, "GET", listed, "");
            assert_eq!(listing.status, 401, "{name} {listed}: {}", listing.body);
        }
        host.stop();
    }
}

#[test]
fn invalid_configuration_reports_every_issue_without_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("merezha.toml");
    fs::write(
        &path,
        r#"[app]
id = "Bad Id"
[http]
listen = "0.0.0.0:8080"
[database]
url = "postgres://user:hunter2@db/app"
colour = "blue"
"#,
    )
    .unwrap();
    let mut host = Host::spawn(&path, &[("MEREZHA_HTTP__BODY_LIMIT", "1")], "invalid");
    let event = host.failed();
    assert_eq!(event["code"], "invalid_config");
    let issues: Vec<(String, String)> = event["issues"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            (
                i["key"].as_str().unwrap().to_owned(),
                i["code"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    for expected in [
        ("app.id", "invalid_app_id"),
        ("http.listen", "public_listen_not_enabled"),
        ("http.body_limit", "out_of_range"),
        ("database.url", "secret_literal"),
        ("database.colour", "unknown_key"),
        ("runtime.manifest", "missing_required"),
    ] {
        assert!(
            issues.contains(&(expected.0.into(), expected.1.into())),
            "missing {expected:?} in {issues:?}"
        );
    }
    assert!(!fs::read_to_string(&host.log).unwrap().contains("hunter2"));
}

#[test]
fn an_application_using_keys_of_later_slices_stops_startup_before_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "");
    let application = dir.path().join("application.toml");
    let text = fs::read_to_string(&application).unwrap().replacen(
        "version=1",
        "version=2\n[limits]\nmax_stream_events=8",
        1,
    );
    fs::write(
        &application,
        format!("{text}[[reactors]]\nname=\"notifier\"\nservice=\"protocol\"\n"),
    )
    .unwrap();
    // The application opens before the pool, so no database is needed.
    let mut host = Host::spawn(
        &config,
        &[(DATABASE_ENV, "postgres://nobody:pw@127.0.0.1:1/none")],
        "unsupported",
    );
    assert_eq!(host.failed()["code"], "unsupported_in_this_build");
}

#[test]
fn an_unreachable_database_stops_startup() {
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "");
    let mut host = Host::spawn(
        &config,
        &[(DATABASE_ENV, "postgres://nobody:pw@127.0.0.1:1/none")],
        "unreachable",
    );
    assert_eq!(host.failed()["code"], "database_unavailable");
    assert!(!fs::read_to_string(&host.log).unwrap().contains(":pw@"));
}

#[test]
fn run_arguments_are_validated() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("arguments.log");
    let status = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["run", "--listen", "127.0.0.1:0"])
        .stderr(File::create(&log).unwrap())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(2));
    let text = fs::read_to_string(&log).unwrap();
    assert!(text.contains("invalid_arguments") && text.contains("merezha run --config"));
}

/// AP-B1 test 3: outbox rows committed while the relay is held (the
/// `MEREZHA_JOBS_TEST_RELAY=off` hook), then kill -9 and a restart: each
/// record is relayed exactly once.
#[test]
fn outbox_records_committed_before_a_crash_are_relayed_exactly_once_after_restart() {
    use merezha_platform::ports::{
        DedupeKey, MessageContext, OnDuplicate, OutboxRecord, OutboxTarget, Source, SourceOutbox,
    };
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "[jobs.relay]\ninterval_ms = 200");
    let mut held = Host::spawn(
        &config,
        &[
            (DATABASE_ENV, test.url()),
            (merezha_jobs::TEST_RELAY_ENV, "off"),
        ],
        "held",
    );
    held.ready();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let db = Database::connect(&test.config()).unwrap();
    let count = |sql: &'static str| {
        runtime.block_on(async {
            let client = db
                .acquire(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
            client.query_one(sql, &[]).await.unwrap().get::<_, i64>(0)
        })
    };
    let outbox = SourceOutbox::new(
        Source::new("tasks").unwrap(),
        Arc::new(merezha_jobs::PgOutbox),
    );
    let records: Vec<OutboxRecord> = (0..20)
        .map(|n| OutboxRecord {
            dedupe_key: DedupeKey::new(format!("reminder:{n}")).unwrap(),
            target: OutboxTarget::Job {
                queue: merezha_domain::Identity::new("default").unwrap(),
                kind: merezha_domain::Identity::new("tasks.remind").unwrap(),
            },
            payload: json!({"n": n}),
            contains_secret: n % 2 == 0,
            run_at: None,
            priority: 0,
            context: MessageContext {
                correlation_id: merezha_domain::Identity::new("c1").unwrap(),
                causation_id: None,
                tenant: None,
            },
        })
        .collect();
    runtime
        .block_on(
            db.transaction(Instant::now() + Duration::from_secs(5), move |tx| {
                Box::pin(async move { outbox.append(tx, &records, OnDuplicate::Fail).await })
            }),
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(
        count("SELECT count(*) FROM merezha_jobs.outbox WHERE relayed_at IS NULL"),
        20,
        "the held relay moved nothing"
    );
    // kill -9: no drain, no shutdown.
    held.child.kill().unwrap();
    held.child.wait().unwrap();

    let mut restarted = Host::spawn(&config, &[(DATABASE_ENV, test.url())], "restarted");
    restarted.ready();
    let deadline = Instant::now() + Duration::from_secs(10);
    while count("SELECT count(*) FROM merezha_jobs.outbox WHERE relayed_at IS NULL") > 0 {
        assert!(Instant::now() < deadline, "records were never relayed");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(count("SELECT count(*) FROM merezha_jobs.jobs"), 20);
    assert_eq!(
        count("SELECT count(DISTINCT outbox_id) FROM merezha_jobs.jobs"),
        20,
        "one job per record"
    );
    assert_eq!(
        count(
            "SELECT count(*) FROM merezha_jobs.outbox WHERE contains_secret AND payload IS NOT NULL"
        ),
        0
    );
    restarted.stop();
}

/// Runs `sql` returning one `bigint` or text cell on a fresh connection.
fn query_cell<T: for<'a> merezha_postgres::tokio_postgres::types::FromSql<'a>>(
    runtime: &tokio::runtime::Runtime,
    db: &Database,
    sql: &str,
) -> T {
    runtime.block_on(async {
        let client = db
            .acquire(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        client.query_one(sql, &[]).await.unwrap().get(0)
    })
}
fn wait_cell(runtime: &tokio::runtime::Runtime, db: &Database, sql: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let now: String = query_cell(runtime, db, sql);
        if now == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "`{sql}` stayed {now}, never {expected}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
/// One `test.sleep` job (the `MEREZHA_JOBS_TEST_HANDLER` kind); its id.
fn insert_sleep_job(runtime: &tokio::runtime::Runtime, db: &Database) -> i64 {
    query_cell(
        runtime,
        db,
        "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest,
             max_attempts, correlation_id)
         VALUES ('default', 'test.sleep', 'jobs:sleep-1', '{}', sha256(convert_to('{}', 'UTF8')), 5, 'c1')
         RETURNING id",
    )
}
fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
/// `outcome` of every lease of job `id`, oldest first, comma-separated.
fn outcomes(runtime: &tokio::runtime::Runtime, db: &Database, id: i64) -> String {
    query_cell(
        runtime,
        db,
        &format!(
            "SELECT string_agg(coalesce(outcome, 'open'), ',' ORDER BY started_at)
               FROM merezha_jobs.attempts WHERE job_id = {id}"
        ),
    )
}

/// AP-B2 test 3, process crash mid-job: kill -9 while the handler runs. The
/// reaper re-offers the job after the lease expires, the lost attempt is
/// counted, and the job runs to success exactly once more.
#[test]
fn a_job_whose_host_is_killed_mid_run_is_reaped_and_runs_once_more() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(
        dir.path(),
        "[jobs]\npoll_interval_ms = 100\nreaper_interval_ms = 200\n\
         [jobs.queues.default]\nlease_ms = 2000\nbackoff_base_ms = 50",
    );
    let mut killed = Host::spawn(
        &config,
        &[
            (DATABASE_ENV, test.url()),
            (merezha_jobs::TEST_HANDLER_ENV, "60000"),
        ],
        "killed",
    );
    killed.ready();
    let runtime = current_thread();
    let db = Database::connect(&test.config()).unwrap();
    let id = insert_sleep_job(&runtime, &db);
    let state = format!("SELECT state FROM merezha_jobs.jobs WHERE id = {id}");
    wait_cell(&runtime, &db, &state, "running");
    // kill -9: no drain, no release; the lease is left to expire.
    killed.child.kill().unwrap();
    killed.child.wait().unwrap();
    assert_eq!(query_cell::<String>(&runtime, &db, &state), "running");

    let mut fresh = Host::spawn(
        &config,
        &[
            (DATABASE_ENV, test.url()),
            (merezha_jobs::TEST_HANDLER_ENV, "50"),
        ],
        "fresh",
    );
    fresh.ready();
    wait_cell(&runtime, &db, &state, "succeeded");
    assert_eq!(
        query_cell::<i32>(
            &runtime,
            &db,
            &format!("SELECT attempts FROM merezha_jobs.jobs WHERE id = {id}")
        ),
        2,
        "the lost attempt counts"
    );
    assert_eq!(outcomes(&runtime, &db, id), "lease_expired,succeeded");
    assert_eq!(
        query_cell::<String>(
            &runtime,
            &db,
            &format!("SELECT last_error_code FROM merezha_jobs.jobs WHERE id = {id}")
        ),
        "lease_expired"
    );
    assert!(
        fresh
            .events()
            .iter()
            .any(|e| e["event"] == "job.retry_scheduled" && e["code"] == "lease_expired"),
        "the reaper logged the reclaim"
    );
    fresh.stop();
}

/// AP-B2 test 5, drain: SIGTERM with a 10 s handler and
/// `drain_timeout_ms = 1000` releases the job with its attempt refunded, the
/// host exits cleanly, and a fresh host runs the job.
#[test]
fn sigterm_releases_a_running_job_with_its_attempt_refunded() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = project(
        dir.path(),
        "[jobs]\npoll_interval_ms = 100\ndrain_timeout_ms = 1000",
    );
    let mut draining = Host::spawn(
        &config,
        &[
            (DATABASE_ENV, test.url()),
            (merezha_jobs::TEST_HANDLER_ENV, "10000"),
        ],
        "draining",
    );
    let address = draining.ready();
    let runtime = current_thread();
    let db = Database::connect(&test.config()).unwrap();
    let id = insert_sleep_job(&runtime, &db);
    let state = format!("SELECT state FROM merezha_jobs.jobs WHERE id = {id}");
    wait_cell(&runtime, &db, &state, "running");
    // The jobs admin API is mounted under /v1/jobs/admin on loopback.
    let queues = request(address, "GET", "/v1/jobs/admin/queues", "");
    assert_eq!(queues.status, 200, "{}", queues.body);
    assert_eq!(queues.body["queues"][0]["name"], "default");
    assert_eq!(queues.body["queues"][0]["leased"], 1);
    let schedules = request(address, "GET", "/v1/jobs/admin/schedules", "");
    assert_eq!(schedules.status, 200, "{}", schedules.body);
    let signalled = Instant::now();
    draining.stop();
    let took = signalled.elapsed();
    assert!(
        took < Duration::from_secs(8),
        "the drain stops at its timeout plus the grace: {took:?}"
    );
    assert_eq!(query_cell::<String>(&runtime, &db, &state), "available");
    assert_eq!(
        query_cell::<i32>(
            &runtime,
            &db,
            &format!("SELECT attempts FROM merezha_jobs.jobs WHERE id = {id}")
        ),
        0,
        "the attempt is refunded"
    );
    assert_eq!(outcomes(&runtime, &db, id), "released");
    let abandoned = draining
        .events()
        .into_iter()
        .find(|e| e["event"] == "job.drain_abandoned")
        .expect("job.drain_abandoned");
    assert_eq!(abandoned["count"], 1);

    let mut fresh = Host::spawn(
        &config,
        &[
            (DATABASE_ENV, test.url()),
            (merezha_jobs::TEST_HANDLER_ENV, "50"),
        ],
        "fresh",
    );
    fresh.ready();
    wait_cell(&runtime, &db, &state, "succeeded");
    assert_eq!(outcomes(&runtime, &db, id), "released,succeeded");
    assert_eq!(
        query_cell::<i32>(
            &runtime,
            &db,
            &format!("SELECT attempts FROM merezha_jobs.jobs WHERE id = {id}")
        ),
        1
    );
    fresh.stop();
}

/// TE §5 and §8 "Health", end to end: `/v1/admin/status` on loopback
/// embeds the jobs admin data as `GET /v1/jobs/admin/queues` and `outbox`
/// return it and carries no planted canary; a ten-minute job backlog
/// leaves readiness at 200 until `jobs.readiness.max_lag_ms` is set.
#[test]
fn status_embeds_the_jobs_admin_data_and_backlog_counts_only_with_max_lag() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    const CANARY: &str = "rUn-StAtUs-CaNaRy";
    let separator = if test.url().contains('?') { '&' } else { '?' };
    let url = format!("{}{separator}application_name={CANARY}", test.url());
    let dir = tempfile::tempdir().unwrap();
    let config = project(dir.path(), "[jobs]\nreaper_interval_ms = 100");
    let mut relaxed = Host::spawn(&config, &[(DATABASE_ENV, url.as_str())], "relaxed");
    let address = relaxed.ready();
    let status = request(
        address,
        "GET",
        &format!("/v1/admin/status?token={CANARY}"),
        "",
    );
    assert_eq!(status.status, 200, "{}", status.body);
    assert!(status.headers.contains("cache-control: no-store"));
    assert!(!status.body.to_string().contains(CANARY));
    let queues = request(address, "GET", "/v1/jobs/admin/queues", "");
    let outbox = request(address, "GET", "/v1/jobs/admin/outbox", "");
    assert_eq!(
        status.body["jobs"],
        json!({"queues": queues.body["queues"], "outbox": outbox.body}),
        "the jobs section is AP's admin data as returned (R10)"
    );
    assert_eq!(status.body["startup"]["state"], "started");
    assert_eq!(status.body["catalog"]["generation"], 1);
    assert_eq!(status.body["catalog"]["services"], 2);
    assert_eq!(status.body["telemetry"]["enabled"], false);
    // The memory application's `Application::status()` (R10).
    assert_eq!(status.body["application"]["head"], 0);
    assert_eq!(
        status.body["application"]["projections"][0]["name"],
        "total"
    );
    let modules: Vec<&str> = status.body["modules"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert_eq!(modules, ["jobs"]);

    // A job no worker serves, due ten minutes ago: the backlog is an
    // alert, not routing.
    let runtime = current_thread();
    let db = Database::connect(&test.config()).unwrap();
    let id: i64 = query_cell(
        &runtime,
        &db,
        "INSERT INTO merezha_jobs.jobs (queue, kind, idempotency_key, payload, payload_digest,
             max_attempts, correlation_id, run_at)
         VALUES ('default', 'tasks.nobody', 'tasks:old', '{}', sha256(convert_to('{}', 'UTF8')), 5,
             'c1', now() - interval '10 minutes')
         RETURNING id",
    );
    std::thread::sleep(Duration::from_millis(2_500));
    assert_eq!(request(address, "GET", "/health/ready", "").status, 200);
    let status = request(address, "GET", "/v1/admin/status", "");
    assert_eq!(status.body["jobs"]["queues"][0]["ready"], 1);
    relaxed.stop();

    let strict_config = project(
        dir.path(),
        "[jobs]\nreaper_interval_ms = 100\n[jobs.readiness]\nmax_lag_ms = 1000",
    );
    let mut strict = Host::spawn(&strict_config, &[(DATABASE_ENV, test.url())], "strict");
    let address = strict.listening();
    strict.wait_for_readiness(address, 503);
    // Still refused once the jobs runtime has synced and measured the lag.
    std::thread::sleep(Duration::from_millis(2_500));
    assert_eq!(request(address, "GET", "/health/ready", "").status, 503);
    let deadline = Instant::now() + Duration::from_secs(5);
    let not_ready = loop {
        // The stderr writer is buffered; wait for the line.
        if let Some(event) = strict
            .events()
            .into_iter()
            .find(|e| e["event"] == "health.not_ready")
        {
            break event;
        }
        assert!(Instant::now() < deadline, "no health.not_ready logged");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(not_ready["check"], "module:jobs", "{not_ready}");
    assert_eq!(not_ready["code"], "jobs_not_ready");
    let _: i64 = query_cell(
        &runtime,
        &db,
        &format!(
            "UPDATE merezha_jobs.jobs SET state = 'cancelled', finished_at = now()
             WHERE id = {id} RETURNING id"
        ),
    );
    strict.wait_for_readiness(address, 200);
    strict.stop();
    let log = fs::read_to_string(&strict.log).unwrap() + &fs::read_to_string(&relaxed.log).unwrap();
    assert!(!log.contains(CANARY), "the database URL was logged");
}
