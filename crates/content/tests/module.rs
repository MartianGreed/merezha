//! The content `Module`: `[modules.content]` and the model file, error
//! registration, and activation and readiness through the host lifecycle.
mod pg;

use axum::Router;
use merezha_content::{ContentHandle, ContentModule, ERRORS};
use merezha_jobs::PgJobs;
use merezha_platform::{
    ConfigInput, ErrorRegistry, Host, HostParts, Module, StartupError, Timeouts, host::configure,
    ports::LoopbackAllowAll,
};
use merezha_postgres::TestDatabase;
use pg::*;
use std::{path::Path, sync::Arc, time::Duration};

/// A project directory with `merezha.toml` (plus `extra` TOML) and the
/// given files.
fn project(dir: &Path, migrate: bool, extra: &str, files: &[(&str, &str)]) {
    for (name, text) in files {
        let path = dir.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    std::fs::write(
        dir.join("merezha.toml"),
        format!(
            "[app]\nid = \"site\"\n[database]\nurl = {{ env = \"CONTENT_DATABASE_URL\" }}\n\
             migrate = {migrate}\npool_size = 6\n[runtime]\nmanifest = \"runtime.toml\"\n{extra}"
        ),
    )
    .unwrap();
}

fn input(dir: &Path, database_url: &str) -> ConfigInput {
    ConfigInput {
        path: dir.join("merezha.toml"),
        env: vec![("CONTENT_DATABASE_URL".into(), database_url.into())],
        overrides: Vec::new(),
    }
}

/// Configure `[modules.content]` and return the issues as `(key, code)`.
fn configure_issues(dir: &Path) -> Result<ContentHandle, Vec<(String, String)>> {
    let module = ContentModule::new();
    let handle = module.handle();
    match configure(
        &input(dir, "postgres://localhost/x"),
        vec![Box::new(module)],
    ) {
        Ok(_) => Ok(handle),
        Err(errors) => Err(errors
            .0
            .iter()
            .map(|i| (i.key.clone(), i.code.to_owned()))
            .collect()),
    }
}

#[test]
fn the_model_path_follows_the_runtime_manifest_rules() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("content.toml"), V1).unwrap();
    let base = dir.path();
    std::fs::create_dir_all(base.join("models/dir.toml")).unwrap();
    std::fs::write(base.join("content.toml"), V1).unwrap();
    std::fs::write(base.join("models/site.toml"), V1).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        outside.path().join("content.toml"),
        base.join("escape.toml"),
    )
    .unwrap();
    let cases: Vec<(String, Option<&str>)> = vec![
        // The default is content.toml beside merezha.toml.
        (String::new(), None),
        ("model = \"models/site.toml\"".into(), None),
        ("model = \"./models/site.toml\"".into(), None),
        ("model = \"../content.toml\"".into(), Some("invalid_path")),
        (
            "model = \"models/../content.toml\"".into(),
            Some("invalid_path"),
        ),
        (
            format!(
                "model = {:?}",
                base.join("content.toml").display().to_string()
            ),
            Some("invalid_path"),
        ),
        ("model = \"models/dir.toml\"".into(), Some("invalid_path")),
        ("model = \"\"".into(), Some("invalid_path")),
        (
            "model = \"missing.toml\"".into(),
            Some("content_model_unreadable"),
        ),
        ("model = 3".into(), Some("expected_string")),
        #[cfg(unix)]
        ("model = \"escape.toml\"".into(), Some("invalid_path")),
    ];
    for (setting, expected) in cases {
        project(base, false, &format!("[modules.content]\n{setting}\n"), &[]);
        match (configure_issues(base), expected) {
            (Ok(handle), None) => {
                let state = handle.state().expect("configured");
                assert_eq!(state.model().version(), 1, "{setting}");
                assert!(state.path().starts_with(base.canonicalize().unwrap()));
            }
            (Err(issues), Some(code)) => assert_eq!(
                issues,
                [("modules.content.model".to_owned(), code.to_owned())],
                "{setting}"
            ),
            (result, _) => panic!("{setting}: {result:?}"),
        }
    }
}

#[test]
fn every_model_problem_is_reported_with_its_toml_path_and_unknown_keys_fail() {
    let dir = tempfile::tempdir().unwrap();
    project(
        dir.path(),
        false,
        "[modules.content]\nmodel = \"content.toml\"\nlimit = 3\n",
        &[(
            "content.toml",
            "version = 1\nlocales = [{ code = \"en\", default = true }]\n\
             [types.page]\nfields = [{ name = \"title\", type = \"string\", max_length = 0 }]\n",
        )],
    );
    let issues = configure_issues(dir.path()).unwrap_err();
    assert_eq!(
        issues,
        [
            ("modules.content.limit".to_owned(), "unknown_key".to_owned()),
            (
                "modules.content.model:types.page.access".to_owned(),
                "content_model_invalid".to_owned()
            ),
            (
                "modules.content.model:types.page.fields[0].max_length".to_owned(),
                "content_model_invalid".to_owned()
            ),
        ]
    );
    // No section, no module: nothing is read.
    project(dir.path(), false, "", &[]);
    let handle = configure_issues(dir.path()).unwrap();
    assert!(handle.state().is_none());
}

#[test]
fn limits_and_revision_retention_are_validated_with_every_problem_at_once() {
    let dir = tempfile::tempdir().unwrap();
    project(
        dir.path(),
        false,
        "[modules.content]\n\
         limits = { max_page_size = 50, default_page_size = 60, max_filters = 0, \
                    query_timeout = \"1h\", max_entry_bytes = \"big\", max_batch = 5 }\n\
         revisions = { retain = 1 }\n",
        &[("content.toml", V1)],
    );
    let issues = configure_issues(dir.path()).unwrap_err();
    let mut issues: Vec<(&str, &str)> = issues
        .iter()
        .map(|(k, c)| (k.as_str(), c.as_str()))
        .collect();
    issues.sort();
    assert_eq!(
        issues,
        [
            ("modules.content.limits.default_page_size", "out_of_range"),
            ("modules.content.limits.max_batch", "unknown_key"),
            ("modules.content.limits.max_entry_bytes", "expected_integer"),
            ("modules.content.limits.max_filters", "out_of_range"),
            ("modules.content.limits.query_timeout", "out_of_range"),
            ("modules.content.revisions.retain", "out_of_range"),
        ]
    );
    project(
        dir.path(),
        false,
        "[modules.content]\nlimits = { max_page_size = 10, query_timeout = \"250ms\" }\n\
         revisions = { retain = 7 }\n",
        &[("content.toml", V1)],
    );
    let handle = configure_issues(dir.path()).unwrap();
    let settings = *handle.state().unwrap().settings();
    assert_eq!(settings.revisions_retain, 7);
    assert_eq!(settings.limits.max_page_size, 10);
    // The default page size follows a lower maximum.
    assert_eq!(settings.limits.default_page_size, 10);
    assert_eq!(settings.limits.query_timeout, Duration::from_millis(250));
    assert_eq!(settings.limits.max_entry_bytes, 262_144);
    project(
        dir.path(),
        false,
        "[modules.content]\nlimits = 3\n",
        &[("content.toml", V1)],
    );
    assert_eq!(
        configure_issues(dir.path()).unwrap_err(),
        [(
            "modules.content.limits".to_owned(),
            "expected_table".to_owned()
        )]
    );
}

#[test]
fn content_errors_register_with_the_kernel_codes() {
    let mut registry = ErrorRegistry::kernel();
    registry.register("content", ERRORS).unwrap();
    for (code, status) in [
        ("entry_not_found", 404),
        ("revision_not_found", 404),
        ("locale_unknown", 400),
        ("invalid_filter", 400),
        ("variant_exists", 412),
        ("revision_conflict", 412),
        ("precondition_required", 428),
        ("entry_exists", 409),
        ("singleton_exists", 409),
        ("content_model_superseded", 503),
        ("content_model_invalid", 422),
        ("content_model_breaking", 422),
        ("index_build_failed", 422),
        ("validation_failed", 422),
    ] {
        assert_eq!(
            registry.status_of(code).map(|s| s.as_u16()),
            Some(status),
            "{code}"
        );
    }
    assert_eq!(ContentModule::new().errors(), ERRORS);
    // Activation's 300 s plus a 10 s margin, beyond the host's 10 s default.
    assert_eq!(
        ContentModule::new().start_timeout(),
        Some(Duration::from_secs(310))
    );
    assert_eq!(ContentModule::new().migrations().len(), 2);
}

fn parts(config: &merezha_platform::ProjectConfig) -> HostParts {
    HostParts {
        routes: Router::new(),
        authorizer: Arc::new(LoopbackAllowAll::new("127.0.0.1:0".parse().unwrap()).unwrap()),
        timeouts: Timeouts {
            drain: Duration::from_secs(5),
            ..Timeouts::default()
        },
        jobs: Some(Arc::new(PgJobs::new(&config.jobs))),
    }
}

/// Whether `host` reports ready within a few seconds (the jobs runtime
/// turns ready after its first subscription sync).
async fn eventually_ready(host: &Host) -> bool {
    for _ in 0..50 {
        if host.readiness().check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Start a host on `dir` with the content module, or the startup error.
async fn start(dir: &Path, test: &TestDatabase) -> Result<(Host, ContentHandle), StartupError> {
    let module = ContentModule::with_options(fast());
    let handle = module.handle();
    let (config, modules) =
        configure(&input(dir, test.url()), vec![Box::new(module)]).expect("configured");
    let parts = parts(&config);
    Host::start(config, modules, parts)
        .await
        .map(|host| (host, handle))
}

#[tokio::test]
async fn the_host_activates_the_model_at_start_and_on_restart() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    project(
        dir.path(),
        true,
        "[modules.content]\n",
        &[("content.toml", V1)],
    );
    let (host, handle) = start(dir.path(), &test).await.unwrap();
    assert!(eventually_ready(&host).await);
    let state = handle.state().unwrap();
    assert!(state.readiness().await.is_ok());
    let db = database(&test).await;
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    assert_eq!(activation_events(&db).await.len(), 1);
    // A second host on the same file is current and changes nothing.
    let (second, _) = start(dir.path(), &test).await.unwrap();
    assert!(eventually_ready(&second).await);
    assert_eq!(activation_events(&db).await.len(), 1);
    drop(second);
    // A restart with v2 activates it; the first host is now outdated.
    let v2 = V1
        .replace("version = 1", "version = 2")
        .replace("max = 600", "max = 900");
    std::fs::write(dir.path().join("content.toml"), &v2).unwrap();
    let (third, handle) = start(dir.path(), &test).await.unwrap();
    assert!(eventually_ready(&third).await);
    assert_eq!(
        models(&db).await,
        [(1, "superseded".to_owned()), (2, "active".to_owned())]
    );
    assert_eq!(
        handle.state().unwrap().model().version(),
        2,
        "the restarted host loaded v2"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!host.readiness().check().await, "the v1 host drains");
    assert_eq!(
        state.readiness().await.unwrap_err().code,
        "content_model_outdated"
    );
    assert!(state.is_outdated());
    // Its writes are refused from now on.
    let mut client = db.acquire(soon()).await.unwrap();
    let mut tx = client.transaction().await.unwrap();
    assert_eq!(
        state.check_writable(&mut tx).await.unwrap_err().code,
        "content_model_superseded"
    );
}

#[tokio::test]
async fn a_refused_change_or_a_failed_build_stops_startup_and_keeps_the_active_model() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    project(
        dir.path(),
        true,
        "[modules.content]\n",
        &[("content.toml", V1)],
    );
    start(dir.path(), &test).await.unwrap();
    let db = database(&test).await;
    let breaking = V1
        .replace("version = 1", "version = 2")
        .replace("  { name = \"slug\", type = \"slug\" },\n", "");
    std::fs::write(dir.path().join("content.toml"), &breaking).unwrap();
    let error = start(dir.path(), &test).await.unwrap_err();
    assert_eq!(error.failure.code, "module_start_failed");
    assert_eq!(error.module, Some("content"));
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    // A new sortable field whose build fails every attempt.
    let indexed = V1.replace("version = 1", "version = 2").replace(
        "  { name = \"seo\", type = \"component\", component = \"seo\" },\n",
        "  { name = \"seo\", type = \"component\", component = \"seo\" },\n  { name = \"rank\", type = \"integer\", sortable = true },\n",
    );
    std::fs::write(dir.path().join("content.toml"), &indexed).unwrap();
    inject_build_failure(&db, "55P03").await;
    let error = start(dir.path(), &test).await.unwrap_err();
    assert_eq!(error.module, Some("content"));
    assert_eq!(build_attempts(&db).await, 3);
    assert_eq!(models(&db).await, [(1, "active".to_owned())]);
    remove_build_failure(&db).await;
    start(dir.path(), &test).await.unwrap();
    assert_eq!(
        models(&db).await,
        [(1, "superseded".to_owned()), (2, "active".to_owned())]
    );
}

#[tokio::test]
async fn without_migrate_a_pending_model_keeps_the_host_not_ready() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    // Migrations and the model applied by another host first.
    let db = database(&test).await;
    run(&db, &model(V1)).await.unwrap();
    let v2 = V1
        .replace("version = 1", "version = 2")
        .replace("max = 600", "max = 900");
    project(
        dir.path(),
        false,
        "[modules.content]\n",
        &[("content.toml", &v2)],
    );
    let (host, handle) = start(dir.path(), &test).await.unwrap();
    assert!(!host.readiness().check().await);
    assert_eq!(
        handle.state().unwrap().readiness().await.unwrap_err().code,
        "content_model_pending"
    );
    // Once a migrating host activated v2, the verifying one turns ready.
    run(&db, &model(&v2)).await.unwrap();
    assert!(eventually_ready(&host).await);
}

#[tokio::test]
async fn check_writable_marks_the_host_outdated_only_when_a_newer_model_is_active() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = database(&test).await;
    run(&db, &model(V1)).await.unwrap();
    // A v2 host configured before anyone activated v2.
    let dir = tempfile::tempdir().unwrap();
    let v2 = V1.replace("version = 1", "version = 2");
    project(
        dir.path(),
        false,
        "[modules.content]\n",
        &[("content.toml", &v2)],
    );
    let state = configure_issues(dir.path()).unwrap().state().unwrap();
    let mut client = db.acquire(soon()).await.unwrap();
    let mut tx = client.transaction().await.unwrap();
    assert_eq!(
        state.check_writable(&mut tx).await.unwrap_err().code,
        "content_model_pending"
    );
    drop(tx);
    assert!(!state.is_outdated(), "a pending model is not outdated");
    // v2 activates: its writes pass.
    run(&db, &model(&v2)).await.unwrap();
    let mut tx = client.transaction().await.unwrap();
    state.check_writable(&mut tx).await.unwrap();
    drop(tx);
    // v3 activates: the v2 host is superseded for good.
    run(&db, &model(&V1.replace("version = 1", "version = 3")))
        .await
        .unwrap();
    let mut tx = client.transaction().await.unwrap();
    assert_eq!(
        state.check_writable(&mut tx).await.unwrap_err().code,
        "content_model_superseded"
    );
    assert!(state.is_outdated());
}
