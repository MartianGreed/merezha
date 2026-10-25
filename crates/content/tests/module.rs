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
fn limits_revision_retention_and_trash_are_validated_with_every_problem_at_once() {
    let dir = tempfile::tempdir().unwrap();
    project(
        dir.path(),
        false,
        "[modules.content]\n\
         limits = { max_page_size = 50, default_page_size = 60, max_filters = 0, \
                    query_timeout = \"1h\", max_entry_bytes = \"big\", max_batch = 501 }\n\
         revisions = { retain = 1 }\n\
         trash = { retention = \"12h\", purge_cron = 5, keep = true }\n",
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
            ("modules.content.limits.max_batch", "out_of_range"),
            ("modules.content.limits.max_entry_bytes", "expected_integer"),
            ("modules.content.limits.max_filters", "out_of_range"),
            ("modules.content.limits.query_timeout", "out_of_range"),
            ("modules.content.revisions.retain", "out_of_range"),
            ("modules.content.trash.keep", "unknown_key"),
            ("modules.content.trash.purge_cron", "expected_string"),
            ("modules.content.trash.retention", "out_of_range"),
        ]
    );
    project(
        dir.path(),
        false,
        "[modules.content]\nlimits = { max_page_size = 10, query_timeout = \"250ms\", max_batch = 20 }\n\
         revisions = { retain = 7 }\ntrash = { retention = \"7d\", purge_cron = \"0 4 * * *\" }\n",
        &[("content.toml", V1)],
    );
    let handle = configure_issues(dir.path()).unwrap();
    let settings = handle.state().unwrap().settings().clone();
    assert_eq!(settings.limits.max_batch, 20);
    assert_eq!(settings.trash.retention, Duration::from_secs(7 * 86_400));
    assert_eq!(settings.trash.purge_cron, "0 4 * * *");
    assert_eq!(settings.revisions_retain, 7);
    assert_eq!(settings.limits.max_page_size, 10);
    // The default page size follows a lower maximum.
    assert_eq!(settings.limits.default_page_size, 10);
    assert_eq!(settings.limits.query_timeout, Duration::from_millis(250));
    assert_eq!(settings.limits.max_entry_bytes, 262_144);
    // The trash defaults: 30 days, daily at 03:23 UTC.
    project(
        dir.path(),
        false,
        "[modules.content]\n",
        &[("content.toml", V1)],
    );
    let defaults = configure_issues(dir.path())
        .unwrap()
        .state()
        .unwrap()
        .settings()
        .clone();
    assert_eq!(defaults.trash.retention, Duration::from_secs(30 * 86_400));
    assert_eq!(defaults.trash.purge_cron, "23 3 * * *");
    assert_eq!(defaults.limits.max_batch, 100);
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

/// A model with an asset field.
const MEDIA_MODEL: &str = r#"
version = 1
locales = [{ code = "en", default = true }]
[types.page]
access = "public"
fields = [{ name = "title", type = "string" }, { name = "image", type = "asset", accept = ["image/*"] }]
"#;

#[test]
fn media_settings_are_validated_with_every_problem_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let files = [("content.toml", MEDIA_MODEL)];
    // Without `[modules.content.media]` asset fields are refused.
    project(dir.path(), false, "[modules.content]\n", &files);
    let issues = configure_issues(dir.path()).unwrap_err();
    assert!(
        issues
            .iter()
            .all(|(_, code)| code == "content_model_invalid"),
        "{issues:?}"
    );
    // Every media problem at once, with its dotted key.
    project(
        dir.path(),
        false,
        "[modules.content.media]\ndriver = \"local\"\nlocal = { root = \"relative\" }\n\
         serve = \"redirect\"\nsigned_url_ttl = \"2h\"\nmax_upload_bytes = 10\n\
         max_concurrent_uploads = 65\nallowed_types = [\"image/png\", \"text/html\", \"Image/PNG\"]\n\
         inline_types = [\"image/svg+xml\"]\nspool_dir = \"spool\"\n\
         asset_cache_control = \"public\\nmax-age=1\"\norphan_grace = \"1m\"\n\
         cleanup_cron = \"never\"\nwatermark = true\n",
        &files,
    );
    let mut issues = configure_issues(dir.path()).unwrap_err();
    issues.sort();
    let key = |k: &str| format!("modules.content.media.{k}");
    let mut expected = vec![
        (key("allowed_types"), "invalid_media_type".to_owned()),
        (key("allowed_types"), "media_type_unsupported".to_owned()),
        (
            key("asset_cache_control"),
            "invalid_header_value".to_owned(),
        ),
        (key("cleanup_cron"), "invalid_cron".to_owned()),
        (key("inline_types"), "inline_type_refused".to_owned()),
        (key("local.root"), "invalid_path".to_owned()),
        (key("max_concurrent_uploads"), "out_of_range".to_owned()),
        (key("max_upload_bytes"), "out_of_range".to_owned()),
        (key("orphan_grace"), "out_of_range".to_owned()),
        (key("serve"), "media_config_invalid".to_owned()),
        (key("signed_url_ttl"), "out_of_range".to_owned()),
        (key("spool_dir"), "invalid_path".to_owned()),
        (key("watermark"), "unknown_key".to_owned()),
    ];
    expected.sort();
    assert_eq!(issues, expected);
    // S3: required keys, nested secrets and the driver's own validation.
    project(
        dir.path(),
        false,
        "[modules.content.media]\ndriver = \"s3\"\nlocal = { root = \"/srv/media\" }\n\
         [modules.content.media.s3]\nbucket = \"Bad_Bucket\"\nendpoint = \"http://127.0.0.1:9000\"\n\
         access_key_id = \"literal-key\"\nsecret_access_key = { env = \"UNSET_S3_SECRET\" }\n\
         timeout = \"10m\"\nacl = \"public\"\n",
        &files,
    );
    let mut issues = configure_issues(dir.path()).unwrap_err();
    issues.sort();
    let mut expected = vec![
        (key("local"), "unused_driver_settings".to_owned()),
        (key("s3.access_key_id"), "secret_literal".to_owned()),
        (key("s3.acl"), "unknown_key".to_owned()),
        (key("s3.bucket"), "invalid_name".to_owned()),
        (key("s3.endpoint"), "plaintext_endpoint_refused".to_owned()),
        (key("s3.region"), "missing_required".to_owned()),
        (key("s3.secret_access_key"), "secret_unavailable".to_owned()),
        (key("s3.timeout"), "out_of_range".to_owned()),
    ];
    expected.sort();
    assert_eq!(issues, expected);
    // A valid S3 section, credentials from files; its Debug redacts them.
    let secret = dir.path().join("secret");
    std::fs::write(&secret, "s3-secret-value\n").unwrap();
    project(
        dir.path(),
        false,
        &format!(
            "[modules.content.media]\ndriver = \"s3\"\nserve = \"redirect\"\n\
             [modules.content.media.s3]\nbucket = \"media\"\nregion = \"eu-west-1\"\n\
             access_key_id = {{ file = {secret:?} }}\nsecret_access_key = {{ file = {secret:?} }}\n"
        ),
        &files,
    );
    let handle = configure_issues(dir.path()).unwrap();
    let state = handle.state().unwrap();
    let media = state.media().unwrap();
    assert_eq!(
        media.settings().serve,
        merezha_content::media::Serve::Redirect
    );
    let merezha_blob::DriverConfig::S3(s3) = &media.settings().driver else {
        panic!("not s3");
    };
    assert_eq!(s3.secret_access_key.expose(), "s3-secret-value");
    assert!(!format!("{:?}", media.settings()).contains("s3-secret-value"));
    // Defaults, with a local root.
    project(
        dir.path(),
        false,
        "[modules.content.media]\nlocal = { root = \"/srv/media\" }\n",
        &files,
    );
    let handle = configure_issues(dir.path()).unwrap();
    let state = handle.state().unwrap();
    let settings = state.media().unwrap().settings();
    assert_eq!(settings.serve, merezha_content::media::Serve::Proxy);
    assert_eq!(settings.max_upload_bytes, 52_428_800);
    assert_eq!(settings.max_concurrent_uploads, 4);
    assert_eq!(settings.signed_url_ttl, Duration::from_secs(300));
    assert_eq!(settings.orphan_grace, Duration::from_secs(86_400));
    assert_eq!(settings.cleanup_cron, "47 3 * * *");
    assert_eq!(settings.asset_cache_control, "public, max-age=3600");
    assert_eq!(settings.allowed_types.len(), 13);
    assert!(settings.inline("image/png"));
    assert!(!settings.inline("text/plain"));
    assert!(!settings.inline("image/svg+xml"));
    assert!(settings.spool_dir.ends_with("merezha-content"));
}

#[test]
fn population_limits_and_delivery_cache_headers_are_validated_with_every_problem_at_once() {
    let dir = tempfile::tempdir().unwrap();
    project(
        dir.path(),
        false,
        "[modules.content]\n\
         limits = { max_populate_depth = 4, max_populated_entries = 0, max_response_bytes = 1024 }\n\
         delivery = { cache_control_public = \"public\\nx\", cache_control_private = 3, \
                      public_base_url = \"https://example.com\" }\n",
        &[("content.toml", V1)],
    );
    let mut issues = configure_issues(dir.path()).unwrap_err();
    issues.sort();
    let issues: Vec<(&str, &str)> = issues
        .iter()
        .map(|(k, c)| (k.as_str(), c.as_str()))
        .collect();
    assert_eq!(
        issues,
        [
            (
                "modules.content.delivery.cache_control_private",
                "expected_string"
            ),
            (
                "modules.content.delivery.cache_control_public",
                "invalid_value"
            ),
            // Asset URLs arrive with C7.
            ("modules.content.delivery.public_base_url", "unknown_key"),
            ("modules.content.limits.max_populate_depth", "out_of_range"),
            (
                "modules.content.limits.max_populated_entries",
                "out_of_range"
            ),
            ("modules.content.limits.max_response_bytes", "out_of_range"),
        ]
    );
    for long in [" public", &"x".repeat(257)] {
        project(
            dir.path(),
            false,
            &format!("[modules.content]\ndelivery = {{ cache_control_public = \"{long}\" }}\n"),
            &[("content.toml", V1)],
        );
        assert!(configure_issues(dir.path()).is_err(), "{long}");
    }
    project(
        dir.path(),
        false,
        "[modules.content]\nlimits = { max_populate_depth = 0, max_populated_entries = 2000 }\n\
         delivery = { cache_control_public = \"public, max-age=5\", \
                      cache_control_private = \"private, max-age=0\" }\n",
        &[("content.toml", V1)],
    );
    let settings = configure_issues(dir.path())
        .unwrap()
        .state()
        .unwrap()
        .settings()
        .clone();
    assert_eq!(settings.limits.max_populate_depth, 0);
    assert_eq!(settings.limits.max_populated_entries, 2000);
    assert_eq!(settings.delivery.cache_control_public, "public, max-age=5");
    assert_eq!(
        settings.delivery.cache_control_private,
        "private, max-age=0"
    );
    let defaults = merezha_content::ContentSettings::default();
    assert_eq!(defaults.limits.max_populate_depth, 2);
    assert_eq!(defaults.limits.max_populated_entries, 500);
    assert_eq!(defaults.limits.max_response_bytes, 16_777_216);
    assert_eq!(
        defaults.delivery.cache_control_public,
        "public, max-age=60, stale-while-revalidate=300"
    );
    assert_eq!(defaults.delivery.cache_control_private, "private, no-cache");
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
        ("entry_referenced", 409),
        ("default_variant_required", 409),
        ("content_model_superseded", 503),
        ("content_model_invalid", 422),
        ("content_model_breaking", 422),
        ("index_build_failed", 422),
        ("validation_failed", 422),
        ("populate_limit", 400),
        ("response_too_large", 400),
        ("content_type_not_found", 404),
    ] {
        assert_eq!(
            registry.status_of(code).map(|s| s.as_u16()),
            Some(status),
            "{code}"
        );
    }
    // The module registers its codes, the asset codes and the blob
    // driver's, all beside the kernel's.
    let errors = ContentModule::new().errors().to_vec();
    assert!(errors.starts_with(ERRORS));
    let mut registry = ErrorRegistry::kernel();
    registry.register("content", &errors).unwrap();
    for (code, status) in [
        ("asset_not_found", 404),
        ("asset_in_use", 409),
        ("asset_too_large", 413),
        ("media_type_rejected", 415),
        ("range_not_satisfiable", 416),
        ("blob_unavailable", 503),
        ("blob_backend_rejected", 502),
        ("blob_root_insecure", 422),
        ("media_config_invalid", 422),
        ("overloaded", 503),
    ] {
        assert_eq!(
            registry.status_of(code).map(|s| s.as_u16()),
            Some(status),
            "{code}"
        );
    }
    // Activation's 300 s plus a 10 s margin, beyond the host's 10 s default.
    assert_eq!(
        ContentModule::new().start_timeout(),
        Some(Duration::from_secs(310))
    );
    assert_eq!(ContentModule::new().migrations().len(), 3);
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
        state.check_writable(&db, &mut tx).await.unwrap_err().code,
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
        state.check_writable(&db, &mut tx).await.unwrap_err().code,
        "content_model_pending"
    );
    drop(tx);
    assert!(!state.is_outdated(), "a pending model is not outdated");
    // v2 activates: its writes pass.
    run(&db, &model(&v2)).await.unwrap();
    let mut tx = client.transaction().await.unwrap();
    state.check_writable(&db, &mut tx).await.unwrap();
    drop(tx);
    // v3 activates: the v2 host is superseded for good.
    run(&db, &model(&V1.replace("version = 1", "version = 3")))
        .await
        .unwrap();
    let mut tx = client.transaction().await.unwrap();
    assert_eq!(
        state.check_writable(&db, &mut tx).await.unwrap_err().code,
        "content_model_superseded"
    );
    assert!(state.is_outdated());
}
